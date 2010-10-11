#include "SDBPShardConnection.h"
#include "SDBPClient.h"
#include "Application/Common/ClientResponse.h"
#include "Application/SDBP/SDBPRequestMessage.h"
#include "Application/SDBP/SDBPResponseMessage.h"

#define CONN_BUFSIZE    4096

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
    // TODO: HACK: see comments in ShardServerApp!
    endpoint.SetPort(endpoint.GetPort() + 1);
    writeBuffer = NULL;
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
    
    // TODO: buffering
//    if (writeBuffer == NULL)
//        writeBuffer = writer->AcquireBuffer(CONN_BUFSIZE);
//    
//    msg.Write(*writeBuffer);
//    request->numTry++;
//
//    if (writeBuffer->GetLength() >= CONN_BUFSIZE)
//    {
//        writer->WritePooled(writeBuffer);
//        writer->Flush();
//        writeBuffer = NULL;
//        return false;
//    }

    return true;
}

void ShardConnection::SendSubmit()
{
    // TODO: submit request
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
    ClientResponse*     resp;
    SDBPResponseMessage msg;
    Request*            it;
    uint64_t            quorumID;
    
    Log_Trace();
    
    resp = new ClientResponse;
    msg.response = resp;
    if (!msg.Read(rbuf))
    {
        delete resp;
        return false;
    }
    
    // find the request in sent requests by commandID
    for (it = sentRequests.First(); it != NULL; it = sentRequests.Next(it))
    {
        if (it->commandID == resp->commandID)
        {
            assert(it == sentRequests.First());
            break;
        }
    }
    
    // not found
    if (it == NULL)
    {
        delete resp;
        return false;
    }

    // TODO: what to do when the first in the queue returns NOSERVICE
    // but the others return OK ?!?

    // TODO: invalidate quorum state on NOSERVICE response
    if (resp->type == CLIENTRESPONSE_NOSERVICE)
    {
        quorumID = it->quorumID;
        InvalidateQuorum(quorumID);
        delete resp;
        return false;
    }
    
    sentRequests.Remove(it);

    if (!client->result->AppendRequestResponse(resp))
        delete resp;        

    return false;
}

void ShardConnection::OnWrite()
{
    MessageConnection::OnWrite();
    SendQuorumRequests();
}

void ShardConnection::OnConnect()
{
    Log_Trace();

    MessageConnection::OnConnect();
    SendQuorumRequests();
}

void ShardConnection::OnClose()
{
    Log_Trace();
    
    Request*    it;
    Request*    prev;
    
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
