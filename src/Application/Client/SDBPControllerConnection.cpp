#include "SDBPControllerConnection.h"
#include "SDBPController.h"
#include "SDBPClient.h"
#include "SDBPClientConsts.h"
#include "Application/Common/ClientRequest.h"
#include "Application/Common/ClientResponse.h"
#include "Application/SDBP/SDBPRequestMessage.h"
#include "Application/SDBP/SDBPResponseMessage.h"
#include "Framework/Replication/PaxosLease/PaxosLease.h"

#define GETMASTER_TIMEOUT   10*1000
#define RECONNECT_TIMEOUT   2000

#define CLIENT_MUTEX_GUARD_DECLARE()    MutexGuard mutexGuard(client->mutex)
#define CLIENT_MUTEX_LOCK()             mutexGuard.Lock()
#define CLIENT_MUTEX_UNLOCK()           mutexGuard.Unlock()

using namespace SDBPClient;

// =============================================================================================
//
// Private & ControllerPool interface
//
// Per connection mutex should NOT be locked here, and pool provides locking
//    
// =============================================================================================

ControllerConnection::ControllerConnection(Controller* controller_, uint64_t nodeID_, Endpoint& endpoint_)
{
    controller = controller_;
    nodeID = nodeID_;
    endpoint = endpoint_;
    getConfigStateTime = 0;
    getConfigStateTimeout.SetDelay(GETMASTER_TIMEOUT);
    getConfigStateTimeout.SetCallable(MFUNC(ControllerConnection, OnGetConfigStateTimeout));
    SetPriority(true);
    Connect();
}

ControllerConnection::~ControllerConnection()
{
    EventLoop::Remove(&getConfigStateTimeout);
}

// =============================================================================================
//
// Client interface -- mutex should be locked
//    
// =============================================================================================

void ControllerConnection::ClearRequests(Client* client)
{
    Request*    request;
    Request*    next;

    MutexGuard  mutexGuard(mutex);
    
    for (request = requests.First(); request; request = next)
    {
        if (request->client == client)
            next = requests.Remove(request);
        else
            next = requests.Next(request);
    }
}

void ControllerConnection::SendRequest(Request* request)
{
    Log_Trace("type = %c, nodeID = %u", request->type, (unsigned) nodeID);

    SDBPRequestMessage  msg;
    MutexGuard          mutexGuard(mutex);

    msg.request = request;
    Write(msg);

    requests.Append(request);
}

uint64_t ControllerConnection::GetNodeID() const
{
    return nodeID;
}

// =============================================================================================
//
// Callback interface -- mutex should be locked
//    
// =============================================================================================

void ControllerConnection::OnGetConfigStateTimeout()
{
    Log_Trace();
        
    MutexGuard  mutexGuard(mutex);

    if (EventLoop::Now() - getConfigStateTime > PAXOSLEASE_MAX_LEASE_TIME * 3)
    {
        Log_Debug("ConfigStateTimeout");
        
        if (!controller->HasMaster())
        {
            OnClose();
            // We need to remove connectTimeout, because Connect() will add it back later
            EventLoop::Remove(&connectTimeout);
            Connect();
        }
        return;
    }
    
    SendGetConfigState();
}

bool ControllerConnection::OnMessage(ReadBuffer& rbuf)
{
    SDBPResponseMessage msg;
    ClientResponse*     resp;
    
    MutexGuard  mutexGuard(mutex);

    Log_Trace();
    
    resp = new ClientResponse;
    msg.response = resp;
    if (msg.Read(rbuf))
    {
        if (resp->type == CLIENTRESPONSE_HELLO)
            delete resp;
        else if (!ProcessResponse(resp))
            delete resp;
    }
    else
        delete resp;

    return false;
}

void ControllerConnection::OnWrite()
{
    Log_Trace();

    MutexGuard  mutexGuard(mutex);

    MessageConnection::OnWrite();
}

void ControllerConnection::OnConnect()
{
    Log_Trace();

    MutexGuard  mutexGuard(mutex);

    MessageConnection::OnConnect();
    SendGetConfigState();

    controller->OnConnected(this);
}

void ControllerConnection::OnClose()
{
    Log_Trace();
    
    MutexGuard      mutexGuard(mutex);

    // TODO: resend requests without response
    if (state == CONNECTED)
    {
        requests.Clear();
    }
        
    // close socket
    MessageConnection::OnClose();
    
    // restart reconnection with timeout
    EventLoop::Reset(&connectTimeout);

    // clear timers
    EventLoop::Remove(&getConfigStateTimeout);
    
    // update the controller connectivity status
    controller->OnDisconnected(this);
}

// =============================================================================================
//
// Private implementation, no mutex should be locked
//    
// =============================================================================================

bool ControllerConnection::ProcessResponse(ClientResponse* resp)
{
    if (resp->type == CLIENTRESPONSE_CONFIG_STATE)
        return ProcessGetConfigState(resp);

    return ProcessCommandResponse(resp);
}

bool ControllerConnection::ProcessGetConfigState(ClientResponse* resp)
{
    ASSERT(resp->configState.Get()->masterID == nodeID);
    EventLoop::Remove(&getConfigStateTimeout);
    
    // copy the config state created on stack in OnMessage
    resp->configState.Get()->Transfer(configState);
    
    controller->SetConfigState(this, &configState);

    return false;
}

bool ControllerConnection::ProcessCommandResponse(ClientResponse* resp)
{
    Log_Trace();

    Request*    req;

    req = RemoveRequest(resp->commandID);

    if (resp->type == CLIENTRESPONSE_NOSERVICE)
    {
        Log_Debug("NOSERVICE");

        controller->OnNoService(this);
        return false;
    }
    
    // pair commands to results
    if (req && req->client)
        controller->OnRequestResponse(req, resp);
    
    return false;
}

Request* ControllerConnection::RemoveRequest(uint64_t commandID)
{
    Request*    it;

    // find the request by commandID
    FOREACH (it, requests)
    {
        if (it->commandID == commandID)
        {
            requests.Remove(it);
            return it;
        }
    }

    return NULL;
}

void ControllerConnection::Connect()
{
    // TODO: MessageConnection::Connect does not support timeout parameter
    //MessageConnection::Connect(endpoint, RECONNECT_TIMEOUT);
    
    MessageConnection::Connect(endpoint);
}

void ControllerConnection::SendGetConfigState()
{
    Request*            request;
    SDBPRequestMessage  msg;
    
    //ClearRequests();

    if (state == CONNECTED)
    {
        request = new Request;
        request->GetConfigState(controller->NextCommandID());
        
        // send request but don't append to the request queue
        msg.request = request;
        
        Write(msg);

        delete request;

        EventLoop::Reset(&getConfigStateTimeout);
    }
    else
        ASSERT_FAIL();
}

