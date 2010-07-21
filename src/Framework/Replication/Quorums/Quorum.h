#ifndef QUORUM_H
#define QUORUM_H

#include "System/Platform.h"

/*
===============================================================================

 Quroum

 An interface for abstracting the voting logic of the Paxos and PaxosLease
 replication algorithms.

===============================================================================
*/

class Quorum
{
public:
	virtual ~Quorum() {}

	virtual unsigned		GetNumNodes() const = 0;
	virtual const uint64_t*	GetNodes() const = 0;
	
	virtual void			RegisterAccepted(uint64_t nodeID) = 0;
	virtual void			RegisterRejected(uint64_t nodeID) = 0;
	virtual void			Reset() = 0;

	virtual bool			IsRoundRejected() const = 0;
	virtual bool			IsRoundAccepted() const = 0;
	virtual bool			IsRoundComplete() const = 0;
};

#endif
