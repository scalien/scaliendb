#include "ConfigHTTPHandler.h"
#include "HTTPControllerSession.h"

void ConfigHTTPHandler::SetConfigServer(ConfigServer* configServer_)
{
    configServer = configServer_;
}

bool ConfigHTTPHandler::HandleRequest(HTTPConnection* conn, HTTPRequest& request)
{
    ConfigHTTPClientSession* session;

    session = new ConfigHTTPClientSession;
    session->SetConfigServer(configServer);
    session->SetConnection(conn);

    return session->HandleRequest(request);
}

