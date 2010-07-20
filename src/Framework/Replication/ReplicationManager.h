#ifndef REPLICATIONMANAGER_H
#define REPLICATIONMANAGER_H

#include "System/Common.h"
#include "System/IO/Endpoint.h"

class ReplicationContext;	// forward
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
	
	Node					GetLocalNode();
	unsigned				GetNodeID() const;
	uint64_t				GetRunID() const;

	void					AddNode(const Node& node);
	Node					GetNode(unsigned nodeID) const;
	
	void					AddContext(ReplicationContext* context);
	ReplicationContext*		GetContext(unsigned contextID) const;

	uint64_t				NextProposalID(uint64_t proposalID)	const;

private:
	unsigned				nodeID;
	uint64_t				runID;	
};

/*
===============================================================================

 Node

===============================================================================
*/

struct Node
{
	unsigned			nodeID;
	Endpoint			endpoint;
};

#endif
