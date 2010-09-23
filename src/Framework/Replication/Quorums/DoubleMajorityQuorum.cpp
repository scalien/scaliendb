#include "DoubleMajorityQuorum.h"
#include "System/Common.h"

DoubleMajorityQuorum::DoubleMajorityQuorum()
{
    numNodes[0] = numNodes[1] = 0;
}

void DoubleMajorityQuorum::AddNode(unsigned group, uint64_t nodeID)
{
    assert(group < 2);
    if (numNodes[1] > 0)
        assert(group == 1); // add nodes in group order!
    assert(numNodes[0] + numNodes[1] < 10);
    assert(numNodes[0] <= SIZE(nodes));
    assert(numNodes[1] <= SIZE(nodes));
    for (unsigned i = 0; i < GetNumNodes(); i++)
    {
        if (nodes[i] == nodeID)
            return;
    }
    nodes[numNodes[0] + numNodes[1]] = nodeID;
    numNodes[group]++;
}

unsigned DoubleMajorityQuorum::GetNumNodes() const
{
    return numNodes[0] + numNodes[1];
}

const uint64_t* DoubleMajorityQuorum::GetNodes() const
{
    return (const uint64_t*) &nodes;
}

QuorumVote* DoubleMajorityQuorum::NewVote() const
{
    unsigned i;
    DoubleMajorityQuorumVote* round;
    
    round = new DoubleMajorityQuorumVote;

    for (i = 0; i < SIZE(round->nodes); i++)
        round->nodes[i] = nodes[i];

    return round;
}

DoubleMajorityQuorumVote::DoubleMajorityQuorumVote()
{
    Reset();
}

void DoubleMajorityQuorumVote::RegisterAccepted(uint64_t nodeID)
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

void DoubleMajorityQuorumVote::RegisterRejected(uint64_t nodeID)
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

void DoubleMajorityQuorumVote::Reset()
{
    numAccepted[0] = numAccepted[1] = 0;
    numRejected[0] = numAccepted[1] = 0;
}

bool DoubleMajorityQuorumVote::IsRejected() const
{
    return ((numRejected[0] >= ceil((double)(numNodes[0]) / 2)) ||
            (numRejected[1] >= ceil((double)(numNodes[1]) / 2)));
}

bool DoubleMajorityQuorumVote::IsAccepted() const
{
    return ((numAccepted[0] >= ceil((double)(numNodes[0]+1) / 2)) &&
            (numAccepted[1] >= ceil((double)(numNodes[1]+1) / 2)));
}

bool DoubleMajorityQuorumVote::IsComplete() const
{
    return (((numAccepted[0] + numRejected[0]) == numNodes[0]) &&
            ((numAccepted[1] + numRejected[1]) == numNodes[1]));
}
