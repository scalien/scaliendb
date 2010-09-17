#ifndef CONTROLLER_H
#define CONTROLLER_H

#include "ConfigMessage.h"
#include "ConfigContext.h"
#include "System/Containers/InList.h"
#include "System/Containers/InSortedList.h"
#include "System/Events/Timer.h"
#include "Application/Common/ClusterContext.h"
#include "State/ConfigState.h"

class ClientSession;	// forward
class PrimaryLease;		// forward
/*
===============================================================================================

 Controller

===============================================================================================
*/

class Controller : public ClusterContext
{
public:
	typedef InList<ConfigMessage> MessageList;
	typedef InSortedList<PrimaryLease> LeaseList;

	void			Init();
	
	/* For the client side */
	bool			IsMasterKnown();
	bool			IsMaster();
	uint64_t		GetMaster();

	bool			ProcessClientCommand(ClientSession* conn, ConfigMessage& message);

	/* For ConfigContext */
	void			OnLearnLease();
	void			OnConfigMessage(ConfigMessage& message);

	// ========================================================================================
	// ClusterContext interface:
	//
	void			OnClusterMessage(uint64_t nodeID, ClusterMessage& msg);
	void			OnIncomingConnectionReady(uint64_t nodeID, Endpoint endpoint);
	void			OnAwaitingNodeID(Endpoint endpoint);
	// ========================================================================================

private:
	void			OnPrimaryLeaseTimeout();
	void			InitConfigContext();
	void			TryRegisterShardServer(Endpoint& endpoint);
	void			SendClientReply(ConfigMessage& message);
	void			OnRequestLease(ClusterMessage& message);
	void			AssignPrimaryLease(ConfigQuorum& quorum, ClusterMessage& message);
	void			ExtendPrimaryLease(ConfigQuorum& quorum, ClusterMessage& message);
	void			UpdatePrimaryLeaseTimer();
	
	ConfigContext	configContext;
	MessageList		configMessages;
	ConfigState		configState;
	LeaseList		primaryLeases;
	Timer			primaryLeaseTimeout;
};

class PrimaryLease
{
public:
	PrimaryLease()	{ prev = next = this; }
	uint64_t		quorumID;
	uint64_t		nodeID;
	uint64_t		expireTime;
	
	PrimaryLease*	prev;
	PrimaryLease*	next;
};

inline bool LessThan(PrimaryLease &a, PrimaryLease &b)
{
	return (a.expireTime < b.expireTime);
}

#endif
