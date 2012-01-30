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

    if (IsMember(nodeID))
        return;

    nodes[numNodes] = nodeID;
    numNodes++;
}

bool MajorityQuorum::IsMember(uint64_t nodeID) const
{
    unsigned i;

    for (i = 0; i < numNodes; i++)
        if (nodes[i] == nodeID)
            return true;

    return false;
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
    
    round = new MajorityQuorumVote((MajorityQuorum*) this);
        
    return round;
}

MajorityQuorumVote::MajorityQuorumVote(MajorityQuorum* quorum_)
{
    quorum = quorum_;
    Reset();
}

void MajorityQuorumVote::RegisterAccepted(uint64_t nodeID)
{
    if (quorum->IsMember(nodeID))
        numAccepted++;
}

void MajorityQuorumVote::RegisterRejected(uint64_t nodeID)
{
    if (quorum->IsMember(nodeID))
        numRejected++;
}

void MajorityQuorumVote::Reset()
{
    numAccepted = 0;
    numRejected = 0;
}

bool MajorityQuorumVote::IsRejected() const
{
    return (numRejected >= ceil((double)(quorum->GetNumNodes()) / 2));
}

bool MajorityQuorumVote::IsAccepted() const
{
    return (numAccepted >= ceil((double)(quorum->GetNumNodes() + 1) / 2));
}

bool MajorityQuorumVote::IsComplete() const
{
    return ((numAccepted + numRejected) == quorum->GetNumNodes());
}
