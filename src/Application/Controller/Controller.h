#ifndef CONTROLLER_H
#define CONTROLLER_H

#include "ConfigMessage.h"
#include "ConfigContext.h"
#include "Application/Common/ClusterContext.h"

class ClientSession; // forward

/*
===============================================================================

 Controller

===============================================================================
*/

class Controller : public ClusterContext
{
public:
	typedef List<ConfigMessage> MessageList;

	void			Init();
	
	/* For the client side */
	bool			IsMasterKnown();
	bool			IsMaster();
	uint64_t		GetMaster();

	bool			ProcessClientCommand(ClientSession* conn, ConfigMessage& message);

	/* For ConfigContext */
	void			OnLearnLease();
	void			OnConfigMessage(ConfigMessage& message);

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
	MessageList		messages;
};

#endif
