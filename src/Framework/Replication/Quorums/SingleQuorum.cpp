#include "SingleQuorum.h"
#include "System/Common.h"

SingleQuorum::SingleQuorum()
{
	numNodes = 0;
}

void SingleQuorum::AddNode(uint64_t nodeID)
{
	if (numNodes >= SIZE(nodes))
		ASSERT_FAIL();
	for (unsigned i = 0; i < GetNumNodes(); i++)
	{
		if (nodes[i] == nodeID)
			return;
	}
	nodes[numNodes] = nodeID;
	numNodes++;
}

unsigned SingleQuorum::GetNumNodes() const
{
	return numNodes;
}

const uint64_t* SingleQuorum::GetNodes() const
{
	return (const uint64_t*) &nodes;
}

QuorumVote* SingleQuorum::NewVote() const
{
	SingleQuorumVote* round;
	
	round = new SingleQuorumVote;
	
	round->numNodes = numNodes;
	
	return round;
}

SingleQuorumVote::SingleQuorumVote()
{
	Reset();
}

void SingleQuorumVote::RegisterAccepted(uint64_t)
{
	numAccepted++;
}

void SingleQuorumVote::RegisterRejected(uint64_t)
{
	numRejected++;
}

void SingleQuorumVote::Reset()
{
	numAccepted = 0;
	numRejected = 0;
}

bool SingleQuorumVote::IsRejected() const
{
	return (numRejected >= ceil((double)(numNodes) / 2));
}

bool SingleQuorumVote::IsAccepted() const
{
	return (numAccepted >= ceil((double)(numNodes+1) / 2));
}

bool SingleQuorumVote::IsComplete() const
{
	return ((numAccepted + numRejected) == numNodes);
}
