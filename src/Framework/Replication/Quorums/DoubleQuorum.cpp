#include "DoubleQuorum.h"
#include "System/Common.h"

DoubleQuorum::DoubleQuorum()
{
	numNodes[0] = numNodes[1] = 0;
	Reset();
}

void DoubleQuorum::AddNode(unsigned group, unsigned nodeID)
{
	assert(group < 2);
	if (numNodes[1] > 0)
		assert(group == 1); // add nodes in group order!
	assert(numNodes[0] + numNodes[1] < 10);
	assert(numNodes[0] <= 5);
	assert(numNodes[1] <= 5);
	nodes[numNodes[0] + numNodes[1]] = nodeID;
	numNodes[group]++;
}

unsigned DoubleQuorum::GetNumNodes() const
{
	return numNodes[0] + numNodes[1];
}

const unsigned* DoubleQuorum::GetNodes() const
{
	return (const unsigned*) &nodes;
}

void DoubleQuorum::RegisterAccepted(unsigned nodeID)
{
#define NID(i) (nodeID == nodes[i])
	switch(numNodes[0])
	{
		case 1:
			if (NID(0))
				numAccepted[0]++;
			else
				numAccepted[1]++;
		case 2:
			if (NID(0) || NID(1))
				numAccepted[0]++;
			else
				numAccepted[1]++;
		case 3:
			if (NID(0) || NID(1) || NID(2))
				numAccepted[0]++;
			else
				numAccepted[1]++;
		case 4:
			if (NID(0) || NID(1) || NID(2) || NID(3))
				numAccepted[0]++;
			else
				numAccepted[1]++;
		case 5:
			if (NID(0) || NID(1) || NID(2) || NID(3) || NID(4))
				numAccepted[0]++;
			else
				numAccepted[1]++;
		default:
			ASSERT_FAIL();
	}
#undef NID
}

void DoubleQuorum::RegisterRejected(unsigned nodeID)
{
#define NID(i) (nodeID == nodes[i])
	switch(numNodes[0])
	{
		case 1:
			if (NID(0))
				numRejected[0]++;
			else
				numRejected[1]++;
		case 2:
			if (NID(0) || NID(1))
				numRejected[0]++;
			else
				numRejected[1]++;
		case 3:
			if (NID(0) || NID(1) || NID(2))
				numRejected[0]++;
			else
				numRejected[1]++;
		case 4:
			if (NID(0) || NID(1) || NID(2) || NID(3))
				numRejected[0]++;
			else
				numRejected[1]++;
		case 5:
			if (NID(0) || NID(1) || NID(2) || NID(3) || NID(4))
				numRejected[0]++;
			else
				numRejected[1]++;
		default:
			ASSERT_FAIL();
	}
#undef NID
}

void DoubleQuorum::Reset()
{
	numAccepted[0] = numAccepted[1] = 0;
	numRejected[0] = numAccepted[1] = 0;
}

bool DoubleQuorum::IsRoundRejected() const
{
	return ((numRejected[0] >= ceil((double)(numNodes[0]) / 2)) ||
			(numRejected[1] >= ceil((double)(numNodes[1]) / 2)));
}

bool DoubleQuorum::IsRoundAccepted() const
{
	return ((numAccepted[0] >= ceil((double)(numNodes[0]+1) / 2)) &&
			(numAccepted[1] >= ceil((double)(numNodes[1]+1) / 2)));
}

bool DoubleQuorum::IsRoundComplete() const
{
	return (((numAccepted[0] + numRejected[0]) == numNodes[0]) &&
			((numAccepted[1] + numRejected[1]) == numNodes[1]));
}
