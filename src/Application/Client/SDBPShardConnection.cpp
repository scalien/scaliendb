#include "SDBPShardConnection.h"
#include "SDBPClient.h"
#include "Application/Common/ClientResponse.h"
#include "Application/SDBP/SDBPRequestMessage.h"
#include "Application/SDBP/SDBPResponseMessage.h"

#define CONN_BUFSIZE    4096

#define CLIENT_MUTEX_GUARD_DECLARE()    MutexGuard mutexGuard(client->mutex)
#define CLIENT_MUTEX_LOCK()             mutexGuard.Lock()
#define CLIENT_MUTEX_UNLOCK()           mutexGuard.Unlock()

using namespace SDBPClient;

static bool LessThan(uint64_t a, uint64_t b)
{
    return a < b;
}

ShardConnection::ShardConnection(Client* client_, uint64_t nodeID_, Endpoint& endpoint_)
{
    client = client_;
    nodeID = nodeID_;
    endpoint = endpoint_;
    autoFlush = false;
    isBulkSent = false;
    Connect();
}

void ShardConnection::Connect()
{
//    Log_Debug("Connecting to %s", endpoint.ToString());
    MessageConnection::Connect(endpoint);
}

bool ShardConnection::SendRequest(Request* request)
{
    SDBPRequestMessage  msg;
    
    if (request->IsList())
        Log_Trace("count: %u, offset: %u", request->count, request->offset);
    
    if (!request->isBulk)
    {
        sentRequests.Append(request);
        request->numTry++;
        request->requestTime = EventLoop::Now();
    }

    msg.request = request;
    Write(msg);

//    if (request->numTry > 1)
//        Log_Debug("Resending, commandID: %U, conn: %s", request->commandID, endpoint.ToString());
    
    //Log_Debug("Sending conn: %s, writeBuffer = %B", endpoint.ToString(), writeBuffer);
    
    // buffer is saturated
    if (writeBuffer->GetLength() >= MESSAGING_BUFFER_THRESHOLD)
        return false;
    
    return true;
}

void ShardConnection::SendSubmit(uint64_t /*quorumID*/)
{
    Flush();
}

void ShardConnection::Flush()
{
    FlushWriteBuffer();
}

bool ShardConnection::HasRequestBuffered()
{
    if (writeBuffer && writeBuffer->GetLength() > 0)
        return true;
    return false;
}

uint64_t ShardConnection::GetNodeID() const
{
    return nodeID;
}

Endpoint& ShardConnection::GetEndpoint()
{
    return endpoint;
}

bool ShardConnection::IsWritePending()
{
    return tcpwrite.active;
}

unsigned ShardConnection::GetNumSentRequests()
{
    return sentRequests.GetLength();
}

void ShardConnection::SetQuorumMembership(uint64_t quorumID)
{
    // SortedList takes care of unique IDs
    quorums.Add(quorumID, true);
}

void ShardConnection::ClearQuorumMembership(uint64_t quorumID)
{
    quorums.Remove(quorumID);
}

void ShardConnection::ClearQuorumMemberships()
{
    quorums.Clear();
}

SortedList<uint64_t>& ShardConnection::GetQuorumList()
{
    return quorums;
}

bool ShardConnection::OnMessage(ReadBuffer& rbuf)
{
    SDBPResponseMessage msg;
    Request*            request;

    CLIENT_MUTEX_GUARD_DECLARE();

    //Log_Debug("Shard conn: %s, message: %R", endpoint.ToString(), &rbuf);
    
    response.Init();
    msg.response = &response;
    if (!msg.Read(rbuf))
        return false;
    
    if (response.type == CLIENTRESPONSE_HELLO)
    {
        Log_Trace("SDBP version: %U, message: %R", response.number, &response.value);
        return false;
    }

    if (response.type == CLIENTRESPONSE_NEXT)
        Log_Trace("NEXT, %U", response.commandID);
    
    // find the request in sent requests by commandID
    FOREACH (request, sentRequests)
    {
        if (request->commandID == response.commandID)
        {
            // TODO: async is broken here somehow
            if (
             response.type != CLIENTRESPONSE_LIST_KEYS && 
             response.type != CLIENTRESPONSE_LIST_KEYVALUES &&
             !(request->type == CLIENTREQUEST_COUNT && response.type == CLIENTRESPONSE_NUMBER))
            {
                sentRequests.Remove(request);
            }
            
            // put back the request to the quorum queue
            // on next config state response the client 
            // will reconfigure the quorums and will resend
            // the requests
            if (response.type == CLIENTRESPONSE_NOSERVICE)
            {
                client->AddRequestToQuorum(request, true);
                // TODO: Reassign request to other quorum (e.g. shard migration) like in NEXT
                return false;
            }
            
            if (response.type == CLIENTRESPONSE_NEXT)
            {
                client->NextRequest(request, response.value, response.number, response.offset);
                client->ReassignRequest(request);
                client->SendQuorumRequests();
                return false;
            }
            
            break;
        }
    }
        
    client->result->AppendRequestResponse(&response);
    response.Init();

    return false;
}

void ShardConnection::OnWrite()
{
    Log_Trace();
    
    CLIENT_MUTEX_GUARD_DECLARE();
    
    MessageConnection::OnWrite();
    if (client->IsBulkLoading() && !isBulkSent)
        isBulkSent = true;

    SendQuorumRequests();
}

void ShardConnection::OnConnect()
{
    Log_Trace();
    //Log_Debug("Shard connection connected, endpoint: %s", endpoint.ToString());

    CLIENT_MUTEX_GUARD_DECLARE();

    MessageConnection::OnConnect();
    if (client->IsBulkLoading() && !isBulkSent)
    {
        SendBulkLoadingRequest();
        return;
    }
    
    SendQuorumRequests();
}

void ShardConnection::OnClose()
{
    Log_Trace("Shard connection closing: %s", endpoint.ToString());
    
    Request*    it;
    Request*    prev;
    uint64_t*   itQuorum;
    uint64_t*   itNext;
    
    CLIENT_MUTEX_GUARD_DECLARE();
    
    isBulkSent = false;
    
    // close the socket and try reconnecting
    MessageConnection::OnClose();

    // restart reconnection with timeout
    EventLoop::Reset(&connectTimeout);
    
    // invalidate quorums
    for (itQuorum = quorums.First(); itQuorum != NULL; itQuorum = itNext)
    {
        itNext = quorums.Next(itQuorum);
        InvalidateQuorum(*itQuorum);
    }
    
    // put back requests that have no response to the client's quorum queue
    for (it = sentRequests.Last(); it != NULL; it = prev)
    {
        prev = sentRequests.Prev(it);
        sentRequests.Remove(it);
        client->AddRequestToQuorum(it, false);
    }
}

void ShardConnection::InvalidateQuorum(uint64_t quorumID)
{
    Request*    it;
    Request*    prev;

    // remove all sent requests and queue it back in quorum
    for (it = sentRequests.Last(); it != NULL; it = prev)
    {
        prev = sentRequests.Prev(it);
        if (it->quorumID == quorumID)
        {
            sentRequests.Remove(it);
            client->AddRequestToQuorum(it, false);
        }
    }
    
    client->InvalidateQuorum(quorumID, nodeID);
}

void ShardConnection::SendQuorumRequests()
{
    uint64_t*   qit;
    
    // notify the client so that it can assign the requests to the connection
    FOREACH (qit, quorums)
        client->SendQuorumRequest(this, *qit);
}

void ShardConnection::SendBulkLoadingRequest()
{
    Request req;

    req.BulkLoading(0);    
    SendRequest(&req);
    Flush();
}
