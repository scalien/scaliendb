#include "SingleQuorum.h"
#include "System/Common.h"

SingleQuorum::SingleQuorum()
{
	numNodes = 0;
	Reset();
}

void SingleQuorum::AddNode(unsigned nodeID)
{
	if (numNodes >= SIZE(nodes))
		ASSERT_FAIL();
	nodes[numNodes] = nodeID;
	numNodes++;
}

unsigned SingleQuorum::GetNumNodes() const
{
	return numNodes;
}

const unsigned* SingleQuorum::GetNodes() const
{
	return (const unsigned*) &nodes;
}

void SingleQuorum::RegisterAccepted(unsigned)
{
	numAccepted++;
}

void SingleQuorum::RegisterRejected(unsigned)
{
	numRejected++;
}

void SingleQuorum::Reset()
{
	numAccepted = 0;
	numRejected = 0;
}

bool SingleQuorum::IsRoundRejected() const
{
	return (numRejected >= ceil((double)(numNodes) / 2));
}

bool SingleQuorum::IsRoundAccepted() const
{
	return (numAccepted >= ceil((double)(numNodes+1) / 2));
}

bool SingleQuorum::IsRoundComplete() const
{
	return ((numAccepted + numRejected) == numNodes);
}
