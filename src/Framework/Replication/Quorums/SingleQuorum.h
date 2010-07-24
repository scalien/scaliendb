#ifndef SINGLEQUORUM_H
#define SINGLEQUORUM_H

#include "Quorum.h"

/*
===============================================================================

 SingleQuroum

 Simple majority vote, the default for Paxos as described by Lamport.

===============================================================================
*/

class SingleQuorum : public Quorum
{
public:
	SingleQuorum();
	
	void				AddNode(uint64_t nodeID);
	unsigned			GetNumNodes() const;
	const uint64_t*		GetNodes() const;
	QuorumVote*			NewVote() const;	

private:
	uint64_t			nodes[5];
	unsigned			numNodes;
	unsigned			numAccepted;
	unsigned			numRejected;
};

/*
===============================================================================

 SingleQuroumRound

===============================================================================
*/

class SingleQuorumVote : public QuorumVote
{
public:
	SingleQuorumVote();
	
	void				RegisterAccepted(uint64_t nodeID);
	void				RegisterRejected(uint64_t nodeID);
	void				Reset();

	bool				IsRoundRejected() const;
	bool				IsRoundAccepted() const;
	bool				IsRoundComplete() const;

private:
	uint64_t			nodes[5];
	unsigned			numNodes;
	unsigned			numAccepted;
	unsigned			numRejected;
	
	friend class SingleQuorum;
};


#endif
