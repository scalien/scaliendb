#include "Application/HTTP/HTTPRequest.h"
#include "Application/HTTP/HTTPConnection.h"
#include "Application/HTTP/HTTPConsts.h"
#include "ShardHTTPHandler.h"
#include "ShardHTTPClientSession.h"

void ShardHTTPHandler::SetShardServer(ShardServer* shardServer_)
{
    shardServer = shardServer_;
}

bool ShardHTTPHandler::HandleRequest(HTTPConnection* conn, HTTPRequest& request)
{
    ShardHTTPClientSession*  session;

    if (HTTPSession::RedirectLocalhost(conn, request))
        return true;

    session = new ShardHTTPClientSession;
    session->SetShardServer(shardServer);
    session->SetConnection(conn);

    return session->HandleRequest(request);
}

