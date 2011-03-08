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
    MessageConnection::InitConnected();
    
    numCompleted = 0;
    connectTimestamp = NowClock();
    server = server_;
    isBulkLoading = false;
    
    socket.GetEndpoint(remoteEndpoint);
    Log_Message("[%s] Client connected", remoteEndpoint.ToString());
}

void SDBPConnection::SetContext(SDBPContext* context_)
{
    context = context_;
}

bool SDBPConnection::OnMessage(ReadBuffer& msg)
{
    SDBPRequestMessage  sdbpRequest;
    SDBPResponseMessage sdbpResponse;
    ClientRequest*      request;

    request = REQUEST_CACHE->CreateRequest();
    request->session = this;
    request->isBulk = isBulkLoading;
    sdbpRequest.request = request;
    if (!sdbpRequest.Read(msg) || !context->IsValidClientRequest(request))
    {
        REQUEST_CACHE->DeleteRequest(request);
        OnClose();
        return true;
    }

    if (request->type == CLIENTREQUEST_BULK_LOADING)
    {
        Log_Debug("[%s] Bulk loading activated", remoteEndpoint.ToString());
        REQUEST_CACHE->DeleteRequest(request);
        isBulkLoading = true;
        return false;
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
    uint64_t    elapsed;
    
    elapsed = NowClock() - connectTimestamp;

    Log_Trace("numpending: %d", numPending);
    Log_Message("[%s] Client disconnected (active: %u seconds, served: %u requests)", 
     remoteEndpoint.ToString(), (unsigned)(elapsed / 1000.0 + 0.5), numCompleted);
    
    context->OnClientClose(this);
    MessageConnection::Close();
    
    if (numPending == 0)
        server->DeleteConn(this);
}

void SDBPConnection::OnComplete(ClientRequest* request, bool last)
{
    SDBPResponseMessage sdbpResponse;

    if (last)
        numPending--;

    if (state == TCPConnection::CONNECTED && request->response.type != CLIENTRESPONSE_NORESPONSE)
    {
        sdbpResponse.response = &request->response;
        Write(sdbpResponse);
        // TODO: HACK
        if (writeBuffer->GetLength() >= MESSAGING_BUFFER_THRESHOLD || last ||
         request->type == CLIENTREQUEST_GET_CONFIG_STATE)
            FlushWriteBuffer();
    }

    if (last)
    {
        REQUEST_CACHE->DeleteRequest(request);
        numCompleted++;
    }
    
    if (numPending == 0 && state == DISCONNECTED)
        server->DeleteConn(this);
}

bool SDBPConnection::IsActive()
{
    if (state == DISCONNECTED)
        return false;

    return true;
}
