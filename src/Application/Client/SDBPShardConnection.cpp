#include "SDBPShardConnection.h"
#include "SDBPPooledShardConnection.h"
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
    conn = NULL;
}

void ShardConnection::ClearRequests()
{
    sentRequests.Clear();
}

bool ShardConnection::SendRequest(Request* request)
{
    SDBPRequestMessage  msg;

    sentRequests.Append(request);
    request->numTry++;
    request->requestTime = EventLoop::Now();

    msg.request = request;
    if (conn == NULL)
    {
        conn = PooledShardConnection::GetConnection(this);
        if (conn == NULL)
            return false;
    }
    
    return conn->SendRequest(msg);
}

void ShardConnection::Flush()
{
    if (conn == NULL)
        return;

    conn->Flush();
}

void ShardConnection::ReleaseConnection()
{
    if (conn == NULL)
        return;
    
    PooledShardConnection::ReleaseConnection(conn);
    conn = NULL;
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
    if (conn == NULL)
        return false;
        
    return conn->IsWritePending();
}

bool ShardConnection::IsConnected()
{
    if (conn == NULL)
    {
        conn = PooledShardConnection::GetConnection(this);
        if (conn == NULL)
            return false;
    }

    return conn->IsConnected();
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

void ShardConnection::ReassignSentRequests()
{
    Request*    request;
    
    FOREACH_LAST (request, sentRequests)
    {
        sentRequests.Remove(request);
        client->AddRequestToQuorum(request, false);
    }
}

bool ShardConnection::OnMessage(ReadBuffer& rbuf)
{
    SDBPResponseMessage msg;
    Request*            request;
    bool                clientLocked;

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
            if (
             response.type != CLIENTRESPONSE_LIST_KEYS && 
             response.type != CLIENTRESPONSE_LIST_KEYVALUES &&
             !(request->type == CLIENTREQUEST_COUNT && response.type == CLIENTRESPONSE_NUMBER))
            {
                sentRequests.Remove(request);
            }
            
            // put back the request to the quorum queue and
            // on the next config state response the client 
            // will reconfigure the quorums and will resend
            // the requests
            if (response.type == CLIENTRESPONSE_NOSERVICE)
            {
                //client->AddRequestToQuorum(request, true);
                client->Lock();
                client->ReassignRequest(request);
                client->SendQuorumRequests();
                client->Unlock();
                return false;
            }
            
            if (response.type == CLIENTRESPONSE_NEXT)
            {
                client->NextRequest(
                 request, response.value, response.endKey, response.prefix,
                 response.number);
                client->Lock();
                client->ReassignRequest(request);
                client->SendQuorumRequests();
                client->Unlock();
                return false;
            }
            
            break;
        }
    }
    
    clientLocked = false;
    if (!client->isDone.IsWaiting())
    {
        client->Lock();
        clientLocked = true;
    }
    
    client->result->AppendRequestResponse(&response);
    response.Init();

    if (client->result->GetTransportStatus() == SDBP_SUCCESS)
    {
        //Log_Debug("Transport status == SDBP_SUCCESS");
        client->TryWake();
    }

    if (clientLocked)
        client->Unlock();

    return false;
}

void ShardConnection::OnWrite()
{
    Log_Trace();
    
    CLIENT_MUTEX_GUARD_DECLARE();
    
    SendQuorumRequests();
}

void ShardConnection::OnConnect()
{
    Log_Trace();
    //Log_Debug("Shard connection connected, endpoint: %s", endpoint.ToString());

    CLIENT_MUTEX_GUARD_DECLARE();

    client->ActivateQuorumMembership(this);
    SendQuorumRequests();
}

void ShardConnection::OnClose()
{
    Log_Debug("Shard connection closing: %s", endpoint.ToString());
    
    uint64_t*   itQuorum;
    uint64_t*   itNext;
    
    CLIENT_MUTEX_GUARD_DECLARE();
       
    // invalidate quorums
    for (itQuorum = quorums.First(); itQuorum != NULL; itQuorum = itNext)
    {
        itNext = quorums.Next(itQuorum);
        InvalidateQuorum(*itQuorum);
    }
    
    // put back requests that have no response to the client's quorum queue
    ReassignSentRequests();
}

void ShardConnection::InvalidateQuorum(uint64_t quorumID)
{
    Request*    it;
    Request*    prev;

    // remove all sent requests and enqueue them back to quorum queue
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
