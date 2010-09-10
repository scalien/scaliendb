#include "SDBPConnection.h"
#include "SDBPServer.h"
#include "SDBPContext.h"

SDBPConnection::SDBPConnection()
{
	server = NULL;
	context = NULL;
}

void SDBPConnection::Init(SDBPServer* server_)
{
	Endpoint remote;

	MessageConnection::InitConnected();
	
	server = server_;
	closeAfterSend = false;
	
	socket.GetEndpoint(remote);
	Log_Message("[%s] Keyspace: client connected", remote.ToString());
}

void SDBPConnection::SetContext(SDBPContext* context_)
{
	context = context_;
}

bool SDBPConnection::OnMessage(ReadBuffer& msg)
{
//	ClientRequest	request;
//	ClientResponse	response;
//	
//	if (!request.Read(msg))
//	{
//		OnClose();
//		return true;
//	}
//
//	if (!context->IsValidRequest(request))
//	{
//		OnClose();
//		return true;
//	}
//
//	if (!context->ProcessRequest(this, request))
//	{
//		response.Failed(request.commandID);
//		SendResponse(response, true);
//	}
//	
//	return false;
}

void SDBPConnection::OnWrite()
{
	Log_Trace();
	
	MessageConnection::OnWrite();
	if (closeAfterSend && !tcpwrite.active)
		OnClose();
}

void SDBPConnection::OnClose()
{
	Endpoint remote;
	
	Log_Trace("numpending: %d", numPendingOps);
	Log_Message("[%s] Keyspace: client disconnected", remote.ToString());

	Close();
	if (numPendingOps == 0)
		server->DeleteConn(this);
}

void SDBPConnection::OnComplete(Command* command)
{
	context->OnComplete(this, command);
}

bool SDBPConnection::IsActive()
{
	if (state == DISCONNECTED)
		return false;

	return true;
}

void SDBPConnection::SendResponse(ClientResponse& response, bool closeAfterSend_)
{
	response.Write(writeBuffer);
	Write(writeBuffer);
	closeAfterSend = closeAfterSend_;
}
