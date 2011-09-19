#include "SDBPConnection.h"
#include "SDBPServer.h"
#include "SDBPContext.h"
#include "SDBPRequestMessage.h"
#include "SDBPResponseMessage.h"
#include "Application/Common/ClientRequestCache.h"
#include "System/Config.h"

static const unsigned keepAliveTimeout = 60*1000; // msec

SDBPConnection::SDBPConnection()
{
    server = NULL;
    context = NULL;
    numPending = 0;
    autoFlush = false;
    onKeepAlive.SetCallable(MFUNC(SDBPConnection, OnKeepAlive));
    onKeepAlive.SetDelay(0);
}

SDBPConnection::~SDBPConnection()
{
    EventLoop::Remove(&onKeepAlive);
}

void SDBPConnection::Init(SDBPServer* server_)
{
    ClientResponse      resp;
    SDBPResponseMessage sdbpResponse;
    
    MessageConnection::InitConnected();
    
    numCompleted = 0;
    connectTimestamp = NowClock();
    server = server_;
    
    socket.GetEndpoint(remoteEndpoint);
    Log_Message("[%s] Client connected", remoteEndpoint.ToString());
    
    resp.Hello();
    sdbpResponse.response = &resp;
    Write(sdbpResponse);
    Flush();

    if (onKeepAlive.GetDelay() > 0)
        EventLoop::Add(&onKeepAlive);
}

void SDBPConnection::SetContext(SDBPContext* context_)
{
    context = context_;
}

bool SDBPConnection::OnMessage(ReadBuffer& msg)
{
    SDBPRequestMessage  sdbpRequest;
    ClientRequest*      request;

    if (onKeepAlive.GetDelay() > 0)
        EventLoop::Reset(&onKeepAlive);

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
    
    if (onKeepAlive.GetDelay() > 0)
        EventLoop::Reset(&onKeepAlive);

    MessageConnection::OnWrite();
}

void SDBPConnection::OnClose()
{
    uint64_t    elapsed;
    
    EventLoop::Remove(&onKeepAlive);
    elapsed = NowClock() - connectTimestamp;

    Log_Message("[%s] Client disconnected (active: %u seconds, served: %u requests)", 
     remoteEndpoint.ToString(), (unsigned)(elapsed / 1000.0 + 0.5), numCompleted);
    
    context->OnClientClose(this);
    MessageConnection::Close();
    
    if (numPending == 0)
    {
        Log_Debug("[%s] Connection deleted", remoteEndpoint.ToString());
        server->DeleteConn(this);
    }
}

void SDBPConnection::OnComplete(ClientRequest* request, bool last)
{
    SDBPResponseMessage sdbpResponse;

    if (onKeepAlive.GetDelay() > 0)
        EventLoop::Reset(&onKeepAlive);

    if (last)
        numPending--;

//    if (request->response.type == CLIENTRESPONSE_OK)
//        Log_Debug("OK, commandID: %U", request->commandID);

    if (state == TCPConnection::CONNECTED &&
     request->response.type != CLIENTRESPONSE_NORESPONSE &&
     !(request->response.type == CLIENTRESPONSE_CONFIG_STATE &&
      TCPConnection::GetWriteBuffer().GetLength() > SDBP_MAX_QUEUED_BYTES))
    {
        sdbpResponse.response = &request->response;
        Write(sdbpResponse);
        // TODO: HACK
        if (TCPConnection::GetWriteBuffer().GetLength() >= MESSAGING_BUFFER_THRESHOLD || last ||
         request->type == CLIENTREQUEST_GET_CONFIG_STATE)
            Flush();
    }

    if (last)
    {
        REQUEST_CACHE->DeleteRequest(request);
        numCompleted++;
    }
    
    if (numPending == 0 && state == DISCONNECTED)
    {
        Log_Debug("[%s] Connection deleted", remoteEndpoint.ToString());
        server->DeleteConn(this);
    }
}

bool SDBPConnection::IsActive()
{
    if (state == DISCONNECTED)
        return false;

    return true;
}

void SDBPConnection::UseKeepAlive(bool useKeepAlive_)
{
    onKeepAlive.SetDelay(configFile.GetIntValue("sdbp.keepAliveTimeout", keepAliveTimeout));
}

void SDBPConnection::OnKeepAlive()
{
    Log_Debug("Keep alive timeout occured");
    OnClose();
}
