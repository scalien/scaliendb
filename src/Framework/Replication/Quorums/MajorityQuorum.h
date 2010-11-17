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
    unsigned            GetNumNodes() const;
    const uint64_t*     GetNodes() const;
    QuorumVote*         NewVote() const;    

private:
    uint64_t            nodes[5];
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
    MajorityQuorumVote();
    
    void                RegisterAccepted(uint64_t nodeID);
    void                RegisterRejected(uint64_t nodeID);
    void                Reset();

    bool                IsRejected() const;
    bool                IsAccepted() const;
    bool                IsComplete() const;

private:
    uint64_t            nodes[5];
    unsigned            numNodes;
    unsigned            numAccepted;
    unsigned            numRejected;
    
    friend class MajorityQuorum;
};


#endif
