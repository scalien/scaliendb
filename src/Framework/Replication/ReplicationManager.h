#ifndef REPLICATIONMANAGER_H
#define REPLICATIONMANAGER_H

#include "System/Common.h"
#include "System/IO/Endpoint.h"

class ReplicationContext;	// forward
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
	
	uint64_t				GetNodeID() const;
	uint64_t				GetRunID() const;

	void					AddNode(uint64_t nodeID, Endpoint endpoint);
	Endpoint				GetNodeEndpoint(uint64_t nodeID) const;
	
	void					AddContext(ReplicationContext* context);
	ReplicationContext*		GetContext(unsigned contextID) const;

	uint64_t				NextProposalID(uint64_t proposalID)	const;
	
	ReplicationTransport*	GetTransport();

private:
	uint64_t				nodeID;
	uint64_t				runID;	
};

#endif
