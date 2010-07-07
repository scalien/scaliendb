#ifndef REPLICATIONMANAGER_H
#define REPLICATIONMANAGER_H

#include "Node.h"

#define	RMAN (ReplicationManager::Get())

class ReplicationContext; // forward

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

#endif
