#include "SDBPControllerConnection.h"
#include "SDBPClient.h"
#include "SDBPClientConsts.h"
#include "Application/Common/ClientRequest.h"
#include "Application/Common/ClientResponse.h"
#include "Application/SDBP/SDBPRequestMessage.h"
#include "Application/SDBP/SDBPResponseMessage.h"
#include "Framework/Replication/PaxosLease/PaxosLease.h"

#define GETMASTER_TIMEOUT   1000
#define RECONNECT_TIMEOUT   2000

using namespace SDBPClient;

ControllerConnection::ControllerConnection(Client* client_, uint64_t nodeID_, Endpoint& endpoint_)
{
    client = client_;
    nodeID = nodeID_;
    endpoint = endpoint_;
    getConfigStateTime = 0;
    getConfigStateTimeout.SetDelay(GETMASTER_TIMEOUT);
    getConfigStateTimeout.SetCallable(MFUNC(ControllerConnection, OnGetConfigStateTimeout));
    Connect();
}

void ControllerConnection::Connect()
{
    // TODO: MessageConnection::Connect does not support timeout parameter
    //MessageConnection::Connect(endpoint, RECONNECT_TIMEOUT);
    
    MessageConnection::Connect(endpoint);
}

void ControllerConnection::Send(ClientRequest* request)
{
    Log_Trace("type = %c, nodeID = %u", request->type, (unsigned) nodeID);

    SDBPRequestMessage  msg;

    // TODO: ClientRequest::nodeID is missing
    msg.request = request;
    Write(msg);
}

void ControllerConnection::SendGetConfigState()
{
    Request*    request;
    
    if (state == CONNECTED)
    {
        request = client->CreateGetConfigState();
        requests.Append(request);
        Send(request);
        EventLoop::Reset(&getConfigStateTimeout);
    }
    else
        ASSERT_FAIL();
}

uint64_t ControllerConnection::GetNodeID()
{
    return nodeID;
}

void ControllerConnection::OnGetConfigStateTimeout()
{
    Log_Trace();
    
    if (EventLoop::Now() - getConfigStateTime > PAXOSLEASE_MAX_LEASE_TIME)
    {
        Log_Trace();
        
        OnClose();
        Connect();
        return;
    }
    
    SendGetConfigState();
}

bool ControllerConnection::OnMessage(ReadBuffer& rbuf)
{
    SDBPResponseMessage msg;
    ClientResponse*     resp;
    
    Log_Trace();
    
    resp = new ClientResponse;
    resp->configState = &tmpState;
    msg.response = resp;
    if (msg.Read(rbuf))
    {
        if (!ProcessResponse(resp))
            delete resp;
    }
    else
        delete resp;
        
    return false;
}

void ControllerConnection::OnWrite()
{
    // TODO: what to do?
}

void ControllerConnection::OnConnect()
{
    Log_Trace();

    MessageConnection::OnConnect();
    client->OnControllerConnected(this);
}

void ControllerConnection::OnClose()
{
    Log_Trace();
    
    Request*    it;
    Request*    next;
    
    // resend requests without response
    if (state == CONNECTED)
    {
        for (it = requests.First(); it != NULL; it = next)
        {
            next = requests.Remove(it);
            client->ReassignRequest(it);
        }
    }
    
    // close socket
    MessageConnection::OnClose();
    
    // clear timers
    EventLoop::Remove(&getConfigStateTimeout);
    
    // update the master in the client
    client->SetMaster(-1, nodeID);
    
    // update the client connectivity status
    client->OnControllerDisconnected(this);
}

bool ControllerConnection::ProcessResponse(ClientResponse* resp)
{
    if (resp->type == CLIENTRESPONSE_GET_CONFIG_STATE)
        return ProcessGetConfigState(resp);

    return ProcessCommandResponse(resp);
}

bool ControllerConnection::ProcessGetConfigState(ClientResponse* resp)
{
    ClientRequest*  req;
    
    assert(resp->configState->masterID == nodeID);
    
    req = RemoveRequest(resp->commandID);
    assert(req != NULL);
    delete req;
    
    // copy the config state created on stack in OnMessage
    resp->configState.Transfer(&configState);
    
    client->SetMaster(configState.masterID, nodeID);
    client->SetConfigState(this, &configState);

    return false;
}

bool ControllerConnection::ProcessGetMaster(ClientResponse* resp)
{
    Log_Trace();

    ClientRequest*  req;
    
    req = RemoveRequest(resp->commandID);
    if (req == NULL)
        return false;   // not found
        
    assert(req->type == CLIENTREQUEST_GET_MASTER);
    delete req;

    getConfigStateTime = EventLoop::Now();
    Log_Trace("getConfigStateTime = %" PRIu64, getConfigStateTime);
    
    if (resp->type == CLIENTRESPONSE_OK)
        client->SetMaster((int64_t) resp->number, nodeID);
    else
        client->SetMaster(-1, nodeID);
        
    return false;
}

bool ControllerConnection::ProcessCommandResponse(ClientResponse* resp)
{
    Log_Trace();
    
    if (resp->type == CLIENTRESPONSE_NOSERVICE)
    {
        Log_Trace("NOSERVICE");
        client->SetMaster(-1, nodeID);
        return false;
    }
    
    // TODO: pair commands to results
    
    return false;
}

Request* ControllerConnection::RemoveRequest(uint64_t commandID)
{
    Request*    it;
    
    // find the request by commandID
    for (it = requests.First(); it != NULL; it = requests.Next(it))
    {
        if (it->commandID == commandID)
        {
            requests.Remove(it);
            break;
        }
    }

    if (it != NULL)
        return it;
        
    return NULL;
}
