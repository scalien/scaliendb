#include "HTTPControllerContext.h"
#include "HTTPControllerSession.h"

void HTTPControllerContext::SetConfigServer(ConfigServer* configServer_)
{
    configServer = configServer_;
}

bool HTTPControllerContext::HandleRequest(HTTPConnection* conn, HTTPRequest& request)
{
    HTTPControllerSession*  session;

    session = new HTTPControllerSession;
    session->SetConfigServer(configServer);
    session->SetConnection(conn);

    return session->HandleRequest(request);
}

