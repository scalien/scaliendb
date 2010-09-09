#include "ReplicationConfig.h"

#define WIDTH_NODEID				16
#define WIDTH_RUNID					16

ReplicationConfig* replicationManager = NULL;

ReplicationConfig* ReplicationConfig::Get()
{
	if (replicationManager == NULL)
		replicationManager = new ReplicationConfig();
	
	return replicationManager;
}

ReplicationConfig::ReplicationConfig()
{
	runID = 0;
}

void ReplicationConfig::SetNodeID(uint64_t nodeID_)
{
	nodeID = nodeID_;
}

uint64_t ReplicationConfig::GetNodeID()
{
	return nodeID;
}

void ReplicationConfig::SetRunID(uint64_t runID_)
{
	runID = runID_;
}

uint64_t ReplicationConfig::GetRunID()
{
	return runID;
}

uint64_t ReplicationConfig::NextProposalID(uint64_t proposalID)
{
	// <proposal count since restart> <runID> <nodeID>
	
	assert(nodeID < (1 << WIDTH_NODEID));
	assert(runID < (1 << WIDTH_RUNID));
	
	uint64_t left, middle, right, nextProposalID;
	
	left = proposalID >> (WIDTH_NODEID + WIDTH_RUNID);
	left++;
	left = left << (WIDTH_NODEID + WIDTH_RUNID);
	middle = runID << WIDTH_NODEID;
	right = nodeID;
	nextProposalID = left | middle | right;
	
	assert(nextProposalID > proposalID);
	
	return nextProposalID;
}
