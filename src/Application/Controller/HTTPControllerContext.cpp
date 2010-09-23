#include "HTTPControllerContext.h"
#include "HTTPControllerSession.h"

void HTTPControllerContext::SetController(Controller* controller_)
{
    controller = controller_;
}

bool HTTPControllerContext::HandleRequest(HTTPConnection* conn, HTTPRequest& request)
{
    HTTPControllerSession*  session;

    session = new HTTPControllerSession;
    session->SetController(controller);
    session->SetConnection(conn);

    return session->HandleRequest(request);
}

