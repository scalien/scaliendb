#include "SDBPControllerContext.h"
#include "SDBPConnection.h"

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
	ConfigCommand command;
	
	if (request.type == CLIENTREQUEST_GET_MASTER)
		return controller->ProcessGetMasterRequest(conn);
	
	command = ConvertRequest(request);
	
	return controller->ProcessCommand(conn, command);
}

void SDBPControllerContext::OnComplete(SDBPConnection* conn, Command& command)
{
	// TODO
}

ConfigCommand SDBPControllerContext::ConvertRequest(ClientRequest& request)
{
	ConfigCommand command;
	
	switch (request.type)
	{
		case CLIENTREQUEST_CREATE_DATABASE:
			command.type = CONFIG_CREATE_DATABASE;
			command.name = request.name;
			break;
		case CLIENTREQUEST_RENAME_DATABASE:
			command.type = CONFIG_RENAME_DATABASE;
			command.databaseID = request.databaseID;
			command.name = request.name;
			break;
		case CLIENTREQUEST_DELETE_DATABASE:
			command.type = CONFIG_DELETE_DATABASE;
			command.databaseID = request.databaseID;
			break;
		case CLIENTREQUEST_CREATE_TABLE:
			command.type = CONFIG_CREATE_TABLE;
			command.databaseID = request.databaseID;
			command.name = request.name;
			break;
		case CLIENTREQUEST_RENAME_TABLE:
			command.type = CONFIG_RENAME_TABLE;
			command.databaseID = request.databaseID;
			command.tableID = request.tableID;
			command.name = request.name;
			break;
		case CLIENTREQUEST_DELETE_TABLE:
			command.type = CONFIG_DELETE_TABLE;
			command.databaseID = request.databaseID;
			command.tableID = request.tableID;
			break;
	}
	
	return command;
}
