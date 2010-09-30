#include "TotalQuorum.h"
#include "System/Common.h"

TotalQuorum::TotalQuorum()
{
    numNodes = 0;
}

void TotalQuorum::AddNode(uint64_t nodeID)
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

void TotalQuorum::ClearNodes()
{
    numNodes = 0;
}

unsigned TotalQuorum::GetNumNodes() const
{
    return numNodes;
}

const uint64_t* TotalQuorum::GetNodes() const
{
    return (const uint64_t*) &nodes;
}

QuorumVote* TotalQuorum::NewVote() const
{
    TotalQuorumVote* round;
    
    round = new TotalQuorumVote;
    
    round->numNodes = numNodes;
    
    return round;
}

TotalQuorumVote::TotalQuorumVote()
{
    Reset();
}

void TotalQuorumVote::RegisterAccepted(uint64_t)
{
    numAccepted++;
}

void TotalQuorumVote::RegisterRejected(uint64_t)
{
    numRejected++;
}

void TotalQuorumVote::Reset()
{
    numAccepted = 0;
    numRejected = 0;
}

bool TotalQuorumVote::IsRejected() const
{
    return (numRejected > 0);
}

bool TotalQuorumVote::IsAccepted() const
{
    return (numAccepted == numNodes);
}

bool TotalQuorumVote::IsComplete() const
{
    return ((numAccepted + numRejected) == numNodes);
}
