#ifndef CONTROLLER_H
#define CONTROLLER_H

#include "ConfigCommand.h"
#include "Framework/Replication/ClusterContext.h"

class ClientConnection; // forward

/*
===============================================================================

 Controller

===============================================================================
*/

class Controller : public ClusterContext
{
public:
	/* For the client side */
	bool	ProcessGetMasterRequest(ClientConnection* conn);
	bool	ProcessCommand(ClientConnection* conn, ConfigCommand& command);

	/* For ConfigContext */
	void	OnConfigCommand(ConfigCommand& command);

	/* ---------------------------------------------------------------------------------------- */
	/* MessageConnection interface:																*/
	/*																							*/
	void OnClusterMessage(ReadBuffer& msg);
	void OnIncomingConnectionReady(uint64_t nodeID, Endpoint endpoint);
	/* ---------------------------------------------------------------------------------------- */

};

#endif
