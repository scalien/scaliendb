#include "SDBPControllerContext.h"
#include "Application/SDBP/SDBPConnection.h"

void SDBPControllerContext::SetController(Controller* controller_)
{
	controller = controller_;
}

bool SDBPControllerContext::IsValidRequest(ClientRequest& request)
{
	 return request.IsControllerRequest();
}

bool SDBPControllerContext::ProcessRequest(SDBPConnection* conn, ClientRequest& request)
{
	ConfigMessage	message;
	ClientResponse	response;
	
	if (request.type == CLIENTREQUEST_GET_MASTER)
	{
		if (!controller->IsMasterKnown())
			response.Failed(request.commandID);
		else
			response.Number(request.commandID, controller->GetMaster());
		conn->Write(response);
	}
	
	message = ConvertRequest(request);
	
	return controller->ProcessClientCommand(conn, message);
}

void SDBPControllerContext::OnComplete(SDBPConnection* conn, Message* message_)
{
	ConfigMessage* message = (ConfigMessage*) message_;
	
	// TODO:
	
	delete message;
}

ConfigMessage SDBPControllerContext::ConvertRequest(ClientRequest& request)
{
	ConfigMessage message;
	
	switch (request.type)
	{
		case CLIENTREQUEST_CREATE_QUORUM:
			message.type = CONFIG_CREATE_QUORUM;
			message.productionType = request.productionType;
			message.nodes = request.nodes;
			break;
		case CLIENTREQUEST_CREATE_DATABASE:
			message.type = CONFIG_CREATE_DATABASE;
			message.name = request.name;
			message.productionType = request.productionType;
			break;
		case CLIENTREQUEST_RENAME_DATABASE:
			message.type = CONFIG_RENAME_DATABASE;
			message.databaseID = request.databaseID;
			message.name = request.name;
			break;
		case CLIENTREQUEST_DELETE_DATABASE:
			message.type = CONFIG_DELETE_DATABASE;
			message.databaseID = request.databaseID;
			break;
		case CLIENTREQUEST_CREATE_TABLE:
			message.type = CONFIG_CREATE_TABLE;
			message.databaseID = request.databaseID;
			message.quorumID = request.quorumID;
			message.name = request.name;
			break;
		case CLIENTREQUEST_RENAME_TABLE:
			message.type = CONFIG_RENAME_TABLE;
			message.databaseID = request.databaseID;
			message.tableID = request.tableID;
			message.name = request.name;
			break;
		case CLIENTREQUEST_DELETE_TABLE:
			message.type = CONFIG_DELETE_TABLE;
			message.databaseID = request.databaseID;
			message.tableID = request.tableID;
			break;
	}
	
	return message;
}
