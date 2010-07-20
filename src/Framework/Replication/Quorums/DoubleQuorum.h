#ifndef DOUBLEQUORUM_H
#define DOUBLEQUORUM_H

#include "Quorum.h"

/*
===============================================================================

 DoubleQuroum

 Consists of two groups, a majority is required in both groups.

===============================================================================
*/

class DoubleQuorum : Quorum
{
public:
	DoubleQuorum();
	
	void				AddNode(unsigned group, uint64_t nodeID);
	// add nodes in group order!
	unsigned			GetNumNodes() const;
	const uint64_t*		GetNodes() const;
	
	void				RegisterAccepted(uint64_t nodeID);
	void				RegisterRejected(uint64_t nodeID);
	void				Reset();

	bool				IsRoundRejected() const;
	bool				IsRoundAccepted() const;	
	bool				IsRoundComplete() const;

private:
	uint64_t			nodes[10]; // 2*5
	unsigned			numNodes[2];
	unsigned			numAccepted[2];
	unsigned			numRejected[2];
};

#endif
