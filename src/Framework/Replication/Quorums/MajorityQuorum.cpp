#include "MajorityQuorum.h"
#include "System/Common.h"

MajorityQuorum::MajorityQuorum()
{
    numNodes = 0;
}

void MajorityQuorum::AddNode(uint64_t nodeID)
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

unsigned MajorityQuorum::GetNumNodes() const
{
    return numNodes;
}

const uint64_t* MajorityQuorum::GetNodes() const
{
    return (const uint64_t*) &nodes;
}

QuorumVote* MajorityQuorum::NewVote() const
{
    MajorityQuorumVote* round;
    
    round = new MajorityQuorumVote;
    
    round->numNodes = numNodes;
    
    return round;
}

MajorityQuorumVote::MajorityQuorumVote()
{
    Reset();
}

void MajorityQuorumVote::RegisterAccepted(uint64_t)
{
    numAccepted++;
}

void MajorityQuorumVote::RegisterRejected(uint64_t)
{
    numRejected++;
}

void MajorityQuorumVote::Reset()
{
    numAccepted = 0;
    numRejected = 0;
}

bool MajorityQuorumVote::IsRejected() const
{
    return (numRejected >= ceil((double)(numNodes) / 2));
}

bool MajorityQuorumVote::IsAccepted() const
{
    return (numAccepted >= ceil((double)(numNodes+1) / 2));
}

bool MajorityQuorumVote::IsComplete() const
{
    return ((numAccepted + numRejected) == numNodes);
}
