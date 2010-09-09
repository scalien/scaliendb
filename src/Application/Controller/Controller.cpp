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

void Controller::OnConfigCommand(ConfigCommand& command)
{
}

void Controller::OnClusterMessage(ReadBuffer& msg)
{
}

void Controller::OnIncomingConnectionReady(uint64_t nodeID, Endpoint endpoint)
{
	if (nodeID == 0)
	{
		// TODO: assign it a nodeID
	}	
}
