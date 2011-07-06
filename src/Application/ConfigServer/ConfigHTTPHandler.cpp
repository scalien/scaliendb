#include "ConfigHTTPHandler.h"
#include "ConfigHTTPClientSession.h"

void ConfigHTTPHandler::SetConfigServer(ConfigServer* configServer_)
{
    configServer = configServer_;
}

bool ConfigHTTPHandler::HandleRequest(HTTPConnection* conn, HTTPRequest& request)
{
    ConfigHTTPClientSession* session;

    if (HTTPSession::RedirectLocalhost(conn, request))
        return true;

    session = new ConfigHTTPClientSession;
    session->SetConfigServer(configServer);
    session->SetConnection(conn);

    return session->HandleRequest(request);
}

