#include "Controller.h"
#include "ClientConnection.h"

bool Controller::ProcessGetMasterRequest(ClientConnection* conn)
{
	return true;
}

bool Controller::ProcessCommand(ClientConnection* conn, ConfigCommand& command)
{
	return true;
}
