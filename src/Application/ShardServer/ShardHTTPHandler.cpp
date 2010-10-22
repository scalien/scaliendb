#include "ShardHTTPHandler.h"
#include "ShardHTTPClientSession.h"

void ShardHTTPHandler::SetShardServer(ShardServer* shardServer_)
{
    shardServer = shardServer_;
}

bool ShardHTTPHandler::HandleRequest(HTTPConnection* conn, HTTPRequest& request)
{
    ShardHTTPClientSession*  session;

    session = new ShardHTTPClientSession;
    session->SetShardServer(shardServer);
    session->SetConnection(conn);

    return session->HandleRequest(request);
}

