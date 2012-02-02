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

    if (IsMember(nodeID))
        return;

    nodes[numNodes] = nodeID;
    numNodes++;
}

bool TotalQuorum::IsMember(uint64_t nodeID) const
{
    unsigned i;

    for (i = 0; i < numNodes; i++)
        if (nodes[i] == nodeID)
            return true;

    return false;
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
    TotalQuorumVote* vote;
    
    vote = new TotalQuorumVote((TotalQuorum*)this);
        
    Log_Trace("creating new vote with numNodes = %u", numNodes);
    
    return vote;
}

TotalQuorumVote::TotalQuorumVote(TotalQuorum* quorum_)
{
    quorum = quorum_;
    Reset();
}

void TotalQuorumVote::RegisterAccepted(uint64_t nodeID)
{
    if (quorum->IsMember(nodeID))
        numAccepted++;
}

void TotalQuorumVote::RegisterRejected(uint64_t nodeID)
{
    if (quorum->IsMember(nodeID))
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
    return (numAccepted == quorum->GetNumNodes());
}

bool TotalQuorumVote::IsComplete() const
{
    return ((numAccepted + numRejected) == quorum->GetNumNodes());
}
