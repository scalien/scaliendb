#ifndef REPLICATIONMANAGER_H
#define REPLICATIONMANAGER_H

#include "System/Common.h"
#include "System/IO/Endpoint.h"
#include "System/Containers/HashMap.h"
#include "ReplicationTransport.h"

class QuorumContext;	// forward
class ReplicationTransport;	// forward
class Node;					// forward

#define	RMAN (ReplicationManager::Get())

/*
===============================================================================

 ReplicationManager

===============================================================================
*/

class ReplicationManager
{
public:
	static ReplicationManager* Get();
	
	ReplicationTransport*	GetTransport();

	void					SetNodeID(uint64_t nodeID);
	uint64_t				GetNodeID() const;
	uint64_t				GetRunID() const;
	
	void					AddContext(QuorumContext* context);
	QuorumContext*			GetContext(uint64_t contextID);

	uint64_t				NextProposalID(uint64_t proposalID);
	
private:
	typedef HashMap<uint64_t, QuorumContext*> ContextMap;

	uint64_t				nodeID;
	uint64_t				runID;
	ReplicationTransport	transport;
	ContextMap				contextMap;
};

#endif
