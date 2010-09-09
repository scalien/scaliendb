#ifndef CONTROLLER_H
#define CONTROLLER_H

#include "ConfigCommand.h"

class ClientConnection; // forward

/*
===============================================================================

 Controller

===============================================================================
*/

class Controller
{
public:
	bool	ProcessGetMasterRequest(ClientConnection* conn);
	bool	ProcessCommand(ClientConnection* conn, ConfigCommand& command);
};

#endif
