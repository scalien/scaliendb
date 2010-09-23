#include "SDBPConnection.h"
#include "SDBPServer.h"
#include "SDBPContext.h"
#include "SDBPRequestMessage.h"
#include "SDBPResponseMessage.h"

SDBPConnection::SDBPConnection()
{
    server = NULL;
    context = NULL;
    numPending = 0;
}

void SDBPConnection::Init(SDBPServer* server_)
{
    Endpoint remote;

    MessageConnection::InitConnected();
    
    server = server_;
    
    socket.GetEndpoint(remote);
    Log_Message("[%s] Keyspace: client connected", remote.ToString());
}

void SDBPConnection::SetContext(SDBPContext* context_)
{
    context = context_;
}

bool SDBPConnection::OnMessage(ReadBuffer& msg)
{
    SDBPRequestMessage  sdbpRequest;
    ClientRequest*      request;

    request = new ClientRequest;
    request->session = this;
    sdbpRequest.request = request;
    if (!sdbpRequest.Read(msg) || !context->IsValidClientRequest(request))
    {
        delete request;
        OnClose();
        return true;
    }
    numPending++;
    context->OnClientRequest(request);
    return false;
}

void SDBPConnection::OnWrite()
{
    Log_Trace();
    
    MessageConnection::OnWrite();
    if (!tcpwrite.active)
        OnClose();
}

void SDBPConnection::OnClose()
{
    Endpoint remote;
    
    Log_Trace("numpending: %d", numPending);
    Log_Message("[%s] Keyspace: client disconnected", remote.ToString());

    Close();
    
    context->OnClientClose(this);
    
    if (numPending == 0)
        server->DeleteConn(this);
}

void SDBPConnection::OnComplete(ClientRequest* request, bool last)
{
    SDBPResponseMessage sdbpResponse;

    if (last)
        numPending--;

    if (state == TCPConnection::CONNECTED)
    {
        sdbpResponse.response = &request->response;
        Write(sdbpResponse);
    }

    if (last)
        delete request;
}

bool SDBPConnection::IsActive()
{
    if (state == DISCONNECTED)
        return false;

    return true;
}
