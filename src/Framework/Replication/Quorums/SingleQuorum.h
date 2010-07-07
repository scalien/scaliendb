#ifndef SINGLEQUORUM_H
#define SINGLEQUORUM_H

#include "Quorum.h"

/*
===============================================================================

 SingleQuroum

 Simple majority vote, the default for Paxos as described by Lamport.

===============================================================================
*/

class SingleQuorum : Quorum
{
public:
	SingleQuorum();
	
	void				AddNode(unsigned nodeID);
	const unsigned*		GetNodes() const;
	unsigned			GetNumNodes() const;
	
	void				RegisterAccepted(unsigned nodeID);
	void				RegisterRejected(unsigned nodeID);
	void				Reset();

	bool				IsRoundRejected() const;
	bool				IsRoundAccepted() const;
	bool				IsRoundComplete() const;

private:
	unsigned			nodes[5];
	unsigned			numNodes;
	unsigned			numAccepted;
	unsigned			numRejected;
};

#endif
