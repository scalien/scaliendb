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
//    submitRequest.Init();
    Connect();
}

void ShardConnection::Connect()
{
    MessageConnection::Connect(endpoint);
}

bool ShardConnection::SendRequest(Request* request)
{
    SDBPRequestMessage  msg;
    
    sentRequests.Append(request);

    msg.request = request;
    Write(msg);
    request->numTry++;
    request->requestTime = EventLoop::Now();
    //request->requestTime = NowClock();
    
    // buffer is saturated
    if (writeBuffer->GetLength() >= MESSAGING_BUFFER_THRESHOLD)
        return false;
    
    return true;
}

void ShardConnection::SendSubmit(uint64_t /*quorumID*/)
{
//    SDBPRequestMessage  msg;
//    
//    // TODO: optimize away submitRequest and msg by writing the buffer in constructor
//    submitRequest.Submit(quorumID);
//    msg.request = &submitRequest;
//    Write(msg);
    
    Flush();
}

void ShardConnection::Flush()
{
    FlushWriteBuffer();
}

uint64_t ShardConnection::GetNodeID()
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
    Request*            it;
    uint64_t            quorumID;

    CLIENT_MUTEX_GUARD_DECLARE();
        
    msg.response = &response;
    if (!msg.Read(rbuf))
        return false;
    
    
    // find the request in sent requests by commandID
    FOREACH (it, sentRequests)
    {
        if (it->commandID == response.commandID)
        {
            // TODO: HACK
            //assert(it == sentRequests.First());
            break;
        }
    }
    
    // not found
    if (it == NULL)
        return false;

    // TODO: what to do when the first in the queue returns NOSERVICE
    // but the others return OK ?!?

    // TODO: invalidate quorum state on NOSERVICE response
    if (response.type == CLIENTRESPONSE_NOSERVICE)
    {
        quorumID = it->quorumID;
        InvalidateQuorum(quorumID);
        return false;
    }
    
    sentRequests.Remove(it);
    client->result->AppendRequestResponse(&response);

    return false;
}

void ShardConnection::OnWrite()
{
    CLIENT_MUTEX_GUARD_DECLARE();
    
    MessageConnection::OnWrite();
    SendQuorumRequests();
}

void ShardConnection::OnConnect()
{
    Log_Trace();

    CLIENT_MUTEX_GUARD_DECLARE();
    
    MessageConnection::OnConnect();
    SendQuorumRequests();
}

void ShardConnection::OnClose()
{
    Log_Trace();
    
    Request*    it;
    Request*    prev;
    
    CLIENT_MUTEX_GUARD_DECLARE();
    
    // close the socket and try reconnecting
    MessageConnection::OnClose();
    
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

    for (it = sentRequests.Last(); it != NULL; it = prev)
    {
        prev = sentRequests.Prev(it);
        if (it->quorumID == quorumID)
        {
            sentRequests.Remove(it);
            client->AddRequestToQuorum(it, false);
        }
    }
    
    client->InvalidateQuorum(quorumID);
}

void ShardConnection::SendQuorumRequests()
{
    uint64_t*   qit;
    
    // notify the client so that it can assign the requests to the connection
    for (qit = quorums.First(); qit != NULL; qit = quorums.Next(qit))
        client->SendQuorumRequest(this, *qit);
}
