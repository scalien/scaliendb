#include "SDBPConnection.h"
#include "SDBPServer.h"
#include "SDBPContext.h"
#include "SDBPRequestMessage.h"
#include "SDBPResponseMessage.h"
#include "Application/Common/ClientRequestCache.h"

SDBPConnection::SDBPConnection()
{
    server = NULL;
    context = NULL;
    numPending = 0;
    autoFlush = false;
}

void SDBPConnection::Init(SDBPServer* server_)
{
    Endpoint remote;

    MessageConnection::InitConnected();
    
    server = server_;
    
    socket.GetEndpoint(remote);
    Log_Message("[%s] SDBP: client connected", remote.ToString());
}

void SDBPConnection::SetContext(SDBPContext* context_)
{
    context = context_;
}

bool SDBPConnection::OnMessage(ReadBuffer& msg)
{
    SDBPRequestMessage  sdbpRequest;
    ClientRequest*      request;

    request = REQUEST_CACHE->CreateRequest();
    request->session = this;
    sdbpRequest.request = request;
    if (!sdbpRequest.Read(msg) || !context->IsValidClientRequest(request))
    {
        REQUEST_CACHE->DeleteRequest(request);
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

    // TODO: why is it closed?
//    if (!tcpwrite.active)
//        OnClose();
}

void SDBPConnection::OnClose()
{
    Endpoint remote;
    
    socket.GetEndpoint(remote);

    Log_Trace("numpending: %d", numPending);
    Log_Message("[%s] SDBP: client disconnected", remote.ToString());

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
        // TODO: HACK
        if (writeBuffer->GetLength() >= MESSAGING_BUFFER_THRESHOLD || last ||
         request->type == CLIENTREQUEST_GET_CONFIG_STATE)
            FlushWriteBuffer();
    }

    if (last)
        REQUEST_CACHE->DeleteRequest(request);
}

bool SDBPConnection::IsActive()
{
    if (state == DISCONNECTED)
        return false;

    return true;
}
