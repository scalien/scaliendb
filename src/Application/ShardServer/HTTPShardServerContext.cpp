#include "HTTPShardServerContext.h"
#include "HTTPShardServerSession.h"

void HTTPShardServerContext::SetShardServer(ShardServer* shardServer_)
{
    shardServer = shardServer_;
}

bool HTTPShardServerContext::HandleRequest(HTTPConnection* conn, HTTPRequest& request)
{
    HTTPShardServerSession*  session;

    session = new HTTPShardServerSession;
    session->SetShardServer(shardServer);
    session->SetConnection(conn);

    return session->HandleRequest(request);
}

