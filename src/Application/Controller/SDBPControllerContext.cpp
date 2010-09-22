#include "SDBPControllerContext.h"
#include "Application/SDBP/SDBPConnection.h"
#include "Controller.h"

void SDBPControllerContext::SetController(Controller* controller_)
{
	controller = controller_;
}

bool SDBPControllerContext::IsValidRequest(ClientRequest* request)
{
	 return request->IsControllerRequest();
}

bool SDBPControllerContext::OnRequest(ClientRequest* request)
{
	ConfigMessage		message;
	ClientResponse	response;
	
	if (!request->IsControllerRequest())
	{
	}
	
	return controller->OnClientRequest(conn, message);
}

void SDBPControllerContext::OnComplete(ClientResponse* response)
{
	// TODO:
}
