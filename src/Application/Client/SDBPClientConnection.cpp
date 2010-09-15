#include "SDBPClientConnection.h"
#include "SDBPClient.h"
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
		request = client->CreateGetMasterRequest();
		getMasterRequests.Append(request);
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
	
	// TODO: connectivity status
//	if (client.connectivityStatus == SCDBCLIENT_NOCONNECTION)
//		client.connectivityStatus == SCDBCLIENT_NOMASTER;
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
	getMasterRequests.Clear();
	
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
	switch (resp->commandID % 10)
	{
	case SDBP_MOD_GETMASTER:
		return ProcessGetMaster(resp);

	case SDBP_MOD_COMMAND:
		return ProcessCommandResponse(resp);
	}
	
	ASSERT_FAIL();
	
	return true;
}

bool SDBPClientConnection::ProcessGetMaster(ClientResponse* resp)
{
	Log_Trace();

	ClientRequest**	it;
	ClientRequest*	req;
	
	// find the request by commandID
	for (it = getMasterRequests.First(); it != NULL; it = getMasterRequests.Next(it))
	{
		if ((*it)->commandID == resp->commandID)
			break;
	}

	if (it == NULL)
		return false;	// not found
		
	req = *it;
	assert(req->type == CLIENTREQUEST_GET_MASTER);

	getMasterTime = EventLoop::Now();
	Log_Trace("getMasterTime = %" PRIu64, getMasterTime);
	
	if (resp->type == CLIENTRESPONSE_OK)
		client->SetMaster((int64_t) resp->number, nodeID);
	else
		client->SetMaster(-1, nodeID);
	
	getMasterRequests.Remove(it);
	delete req;
	
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
