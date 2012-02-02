#ifndef MAJORITYQUORUM_H
#define MAJORITYQUORUM_H

#include "Quorum.h"

/*
===============================================================================================

 MajorityQuorum

 Simple majority vote, the default for Paxos as described by Lamport.

===============================================================================================
*/

class MajorityQuorum : public Quorum
{
public:
    MajorityQuorum();
    
    void                AddNode(uint64_t nodeID);
    bool                IsMember(uint64_t nodeID) const;
    unsigned            GetNumNodes() const;
    const uint64_t*     GetNodes() const;
    QuorumVote*         NewVote() const;    

private:
    uint64_t            nodes[9];
    unsigned            numNodes;
    unsigned            numAccepted;
    unsigned            numRejected;
};

/*
===============================================================================================

 MajorityQuorumVote

===============================================================================================
*/

class MajorityQuorumVote : public QuorumVote
{
public:
    MajorityQuorumVote(MajorityQuorum* quorum);
    
    void                RegisterAccepted(uint64_t nodeID);
    void                RegisterRejected(uint64_t nodeID);
    void                Reset();

    bool                IsRejected() const;
    bool                IsAccepted() const;
    bool                IsComplete() const;

private:
    MajorityQuorum*     quorum;
    unsigned            numAccepted;
    unsigned            numRejected;
};


#endif
