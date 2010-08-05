#include "ReplicationManager.h"
#include "Framework/Replication/Quorums/QuorumContext.h"

#define WIDTH_NODEID				16
#define WIDTH_RUNID					16

ReplicationManager* replicationManager = NULL;

static uint64_t Hash(uint64_t ID)
{
	return ID;
}

ReplicationManager* ReplicationManager::Get()
{
	if (replicationManager == NULL)
		replicationManager = new ReplicationManager();
	
	return replicationManager;
}

ReplicationManager::ReplicationManager()
{
	runID = 0;
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

void ReplicationManager::SetRunID(uint64_t runID_)
{
	runID = runID_;
}

uint64_t ReplicationManager::GetRunID() const
{
	return runID;
}

void ReplicationManager::AddContext(QuorumContext* context)
{
	uint64_t contextID = context->GetContextID();
	contextMap.Set(contextID, context);
}

QuorumContext* ReplicationManager::GetContext(uint64_t contextID)
{
	QuorumContext** pcontext;
	
	pcontext = contextMap.Get(contextID);
	assert(pcontext != NULL);
	
	return *pcontext;
}

uint64_t ReplicationManager::NextProposalID(uint64_t proposalID)
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
