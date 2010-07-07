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

Node ReplicationManager::GetLocalNode()
{
	return Node(); // TODO
}

unsigned ReplicationManager::GetNodeID() const
{
	return 0; // TODO
}

uint64_t ReplicationManager::GetRunID() const
{
	return 0; // TODO
}

void ReplicationManager::AddNode(const Node& node)
{
}

Node ReplicationManager::GetNode(unsigned nodeID) const
{
}

//void ReplicationManager::AddContext(ReplicationContext* context)
//{
//}
//
//ReplicationContext ReplicationManager::GetContext(unsigned contextID) const
//{
//}

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
