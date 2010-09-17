#include "SDBPClientConnection.h"
#include "SDBPClient.h"
#include "SDBPClientConsts.h"
#include "Application/Common/ClientRequest.h"
#include "Application/Common/ClientResponse.h"
#include "Framework/Replication/PaxosLease/PaxosLease.h"

#define GETMASTER_TIMEOUT	1000
#define RECONNECT_TIMEOUT	2000

void SDBPClientConnection::Init(SDBPClient* client_, uint64_t nodeID_, Endpoint& endpoint_)
{
	client = client_;
	nodeID = nodeID_;
	endpoint = endpoint_;
	getMasterTime = 0;
	getMasterTimeout.SetDelay(GETMASTER_TIMEOUT);
	getMasterTimeout.SetCallable(MFUNC(SDBPClientConnection, OnGetMasterTimeout));
	Connect();
}

void SDBPClientConnection::Connect()
{
	// TODO: MessageConnection::Connect does not support timeout parameter
	//MessageConnection::Connect(endpoint, RECONNECT_TIMEOUT);
	
	MessageConnection::Connect(endpoint);
}

void SDBPClientConnection::Send(ClientRequest* request)
{
	Log_Trace("type = %c, nodeID = %u", request->type, (unsigned) nodeID);

	// TODO: ClientRequest::nodeID is missing
	
	Write(*request);
}

void SDBPClientConnection::SendGetMaster()
{
	ClientRequest*	request;
	
	if (state == CONNECTED)
	{
		request = client->CreateGetConfigState();
		requests.Append(request);
		Send(request);
		EventLoop::Reset(&getMasterTimeout);
	}
	else
		ASSERT_FAIL();
}

void SDBPClientConnection::OnGetMasterTimeout()
{
	Log_Trace();
	
	if (EventLoop::Now() - getMasterTime > PAXOSLEASE_MAX_LEASE_TIME)
	{
		Log_Trace();
		
		OnClose();
		Connect();
		return;
	}
	
	SendGetMaster();
}

bool SDBPClientConnection::OnMessage(ReadBuffer& msg)
{
	ClientResponse*	resp;
	
	Log_Trace();
	
	resp = new ClientResponse;
	if (resp->Read(msg))
	{
		if (!ProcessResponse(resp))
			delete resp;
	}
	else
		delete resp;
		
	return false;
}

void SDBPClientConnection::OnConnect()
{
	Log_Trace();

	MessageConnection::OnConnect();
	SendGetMaster();
	
	if (client->connectivityStatus == SDBP_NOCONNECTION)
		client->connectivityStatus = SDBP_NOMASTER;
}

void SDBPClientConnection::OnConnectTimeout()
{
	Log_Trace();
	
	OnClose();
	Connect();
}

void SDBPClientConnection::OnClose()
{
	Log_Trace();
	
	// delete getmaster requests
	requests.Clear();
	
	// resend requests without response from the client
	if (state == CONNECTED)
		client->ResendRequests(nodeID);
	
	// close socket
	Close();
	
	// clear timers
	EventLoop::Remove(&getMasterTimeout);
	EventLoop::Reset(&connectTimeout);
	
	// update the master in the client
	client->SetMaster(-1, nodeID);
	
	// update the client connectivity status
	client->UpdateConnectivityStatus();
}

bool SDBPClientConnection::ProcessResponse(ClientResponse* resp)
{
	if (resp->type == CLIENTRESPONSE_GET_CONFIG_STATE)
		return ProcessGetConfigState(resp);

	return ProcessCommandResponse(resp);
}

bool SDBPClientConnection::ProcessGetConfigState(ClientResponse* resp)
{
	ClientRequest*	req;

	if (resp->configState)
	{
		assert(resp->configState->nodeID == nodeID);
		
		req = RemoveRequest(resp->commandID);
		assert(req != NULL);
		delete req;
		
		client->SetMaster(resp->configState->nodeID, nodeID);
		client->SetConfigState(resp->TransferConfigState());
	}
	return false;
}

bool SDBPClientConnection::ProcessGetMaster(ClientResponse* resp)
{
	Log_Trace();

	ClientRequest*	req;
	
	req = RemoveRequest(resp->commandID);
	if (req == NULL)
		return false;	// not found
		
	assert(req->type == CLIENTREQUEST_GET_MASTER);
	delete req;

	getMasterTime = EventLoop::Now();
	Log_Trace("getMasterTime = %" PRIu64, getMasterTime);
	
	if (resp->type == CLIENTRESPONSE_OK)
		client->SetMaster((int64_t) resp->number, nodeID);
	else
		client->SetMaster(-1, nodeID);
		
	return false;
}

bool SDBPClientConnection::ProcessCommandResponse(ClientResponse* resp)
{
	Log_Trace();
	
	if (resp->type == CLIENTRESPONSE_NOTMASTER)
	{
		Log_Trace("NOTMASTER");
		client->SetMaster(-1, nodeID);
		return false;
	}
	
	// TODO: pair commands to results
	
	return false;
}

ClientRequest* SDBPClientConnection::RemoveRequest(uint64_t commandID)
{
	ClientRequest**	it;
	
	// find the request by commandID
	for (it = requests.First(); it != NULL; it = requests.Next(it))
	{
		if ((*it)->commandID == commandID)
		{
			requests.Remove(it);
			break;
		}
	}

	if (it != NULL)
		return *it;
		
	return NULL;
}
