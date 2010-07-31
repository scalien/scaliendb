#ifndef DOUBLEQUORUM_H
#define DOUBLEQUORUM_H

#include "Quorum.h"

#define QUORUM_CONTROL_NODE		0
#define QUORUM_DATA_NODE		1

/*
===============================================================================

 DoubleQuroum

 Consists of two groups, a majority is required in both groups.

===============================================================================
*/

class DoubleQuorum : public Quorum
{
public:
	DoubleQuorum();
	
	void				AddNode(unsigned group, uint64_t nodeID);
	// add nodes in group order!
	unsigned			GetNumNodes() const;
	const uint64_t*		GetNodes() const;
	QuorumVote*			NewVote() const;	
	
private:
	uint64_t			nodes[14]; // 2*7
	unsigned			numNodes[2];
	unsigned			numAccepted[2];
	unsigned			numRejected[2];
};

/*
===============================================================================

 DoubleQuroumRound

===============================================================================
*/

class DoubleQuorumVote : public QuorumVote
{
public:
	DoubleQuorumVote();
	
	void				RegisterAccepted(uint64_t nodeID);
	void				RegisterRejected(uint64_t nodeID);
	void				Reset();

	bool				IsRejected() const;
	bool				IsAccepted() const;	
	bool				IsComplete() const;

private:
	uint64_t			nodes[14]; // 2*7
	unsigned			numNodes[2];
	unsigned			numAccepted[2];
	unsigned			numRejected[2];

	friend class DoubleQuorum;
};


#endif
