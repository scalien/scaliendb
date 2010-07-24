#include "DoubleQuorum.h"
#include "System/Common.h"

DoubleQuorum::DoubleQuorum()
{
	numNodes[0] = numNodes[1] = 0;
}

void DoubleQuorum::AddNode(unsigned group, uint64_t nodeID)
{
	assert(group < 2);
	if (numNodes[1] > 0)
		assert(group == 1); // add nodes in group order!
	assert(numNodes[0] + numNodes[1] < 10);
	assert(numNodes[0] <= SIZE(nodes));
	assert(numNodes[1] <= SIZE(nodes));
	nodes[numNodes[0] + numNodes[1]] = nodeID;
	numNodes[group]++;
}

unsigned DoubleQuorum::GetNumNodes() const
{
	return numNodes[0] + numNodes[1];
}

const uint64_t* DoubleQuorum::GetNodes() const
{
	return (const uint64_t*) &nodes;
}

QuorumVote* DoubleQuorum::NewVote() const
{
	unsigned i;
	DoubleQuorumVote* round;
	
	round = new DoubleQuorumVote;

	for (i = 0; i < SIZE(round->nodes); i++)
		round->nodes[i] = nodes[i];

	return round;
}

DoubleQuorumVote::DoubleQuorumVote()
{
	Reset();
}

void DoubleQuorumVote::RegisterAccepted(uint64_t nodeID)
{
	unsigned i;
	
	for (i = 0; i < numNodes[0]; i++)
	{
		if (nodes[i] == nodeID)
		{
			numAccepted[0]++;
			return;
		}
	}
	
	numAccepted[1]++;
}

void DoubleQuorumVote::RegisterRejected(uint64_t nodeID)
{
	unsigned i;
	
	for (i = 0; i < numNodes[0]; i++)
	{
		if (nodes[i] == nodeID)
		{
			numRejected[0]++;
			return;
		}
	}
	
	numRejected[1]++;
}

void DoubleQuorumVote::Reset()
{
	numAccepted[0] = numAccepted[1] = 0;
	numRejected[0] = numAccepted[1] = 0;
}

bool DoubleQuorumVote::IsRoundRejected() const
{
	return ((numRejected[0] >= ceil((double)(numNodes[0]) / 2)) ||
			(numRejected[1] >= ceil((double)(numNodes[1]) / 2)));
}

bool DoubleQuorumVote::IsRoundAccepted() const
{
	return ((numAccepted[0] >= ceil((double)(numNodes[0]+1) / 2)) &&
			(numAccepted[1] >= ceil((double)(numNodes[1]+1) / 2)));
}

bool DoubleQuorumVote::IsRoundComplete() const
{
	return (((numAccepted[0] + numRejected[0]) == numNodes[0]) &&
			((numAccepted[1] + numRejected[1]) == numNodes[1]));
}
