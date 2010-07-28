#include "ReplicationManager.h"

#define WIDTH_NODEID				16
#define WIDTH_RESTART_COUNTER		16

ReplicationManager* replicationManager = NULL;

ReplicationManager* ReplicationManager::Get()
{
	if (replicationManager == NULL)
		replicationManager = new ReplicationManager();
	
	return replicationManager;
}

ReplicationTransport* ReplicationManager::GetTransport()
{
	return &transport;
}

void ReplicationManager::SetNodeID(uint64_t nodeID_)
{
	nodeID = nodeID_;
}

uint64_t ReplicationManager::GetNodeID() const
{
	return nodeID;
}

uint64_t ReplicationManager::GetRunID() const
{
	// TODO

	return 0;
}

//void ReplicationManager::AddContext(QuorumContext* context)
//{
//}

QuorumContext* ReplicationManager::GetContext(unsigned contextID) const
{
	// TODO
}

uint64_t ReplicationManager::NextProposalID(uint64_t proposalID) const
{
	// <proposal count since restart> <runID> <nodeID>
	
	uint64_t left, middle, right, nextProposalID;
	
	left = proposalID >> (WIDTH_NODEID + WIDTH_RESTART_COUNTER);
	left++;
	left = left << (WIDTH_NODEID + WIDTH_RESTART_COUNTER);
	middle = runID << WIDTH_NODEID;
	right = nodeID;
	nextProposalID = left | middle | right;
	
	return nextProposalID;
}
