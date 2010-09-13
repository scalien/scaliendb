#ifndef CONTROLLER_H
#define CONTROLLER_H

#include "ConfigCommand.h"
#include "ConfigContext.h"
#include "Application/Common/ClusterContext.h"
#include "State/StateProcessor.h"

class ClientConnection; // forward

/*
===============================================================================

 Controller

===============================================================================
*/

class Controller : public ClusterContext
{
public:
	typedef List<ConfigCommand> CommandList;

	void			Init();
	
	/* For the client side */
	bool			IsMasterKnown();
	bool			IsMaster();
	uint64_t		GetMaster();

	bool			ProcessClientCommand(ClientConnection* conn, ConfigCommand& command);

	/* For ConfigContext */
	void			OnLearnLease();
	void			OnConfigCommand(ConfigCommand& command);

	/* ---------------------------------------------------------------------------------------- */
	/* ClusterContext interface:																*/
	/*																							*/
	void			OnClusterMessage(uint64_t nodeID, ClusterMessage& msg);
	void			OnIncomingConnectionReady(uint64_t nodeID, Endpoint endpoint);
	void			OnAwaitingNodeID(Endpoint endpoint);
	/* ---------------------------------------------------------------------------------------- */

private:
	void			InitConfigContext();
	void			TryRegisterShardServer(Endpoint& endpoint);
	
	uint64_t		nextNodeID;
	ConfigContext	configContext;
	StateProcessor	stateProcessor;
	CommandList		commands;
};

#endif
