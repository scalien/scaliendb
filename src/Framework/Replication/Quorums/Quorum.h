#ifndef QUORUM_H
#define QUORUM_H

#include "System/Platform.h"

class QuorumVote; // forward

/*
===============================================================================

 Quorum

===============================================================================
*/

class Quorum
{
public:
	virtual ~Quorum() {}

	virtual unsigned			GetNumNodes()		const	= 0;
	virtual const uint64_t*		GetNodes()			const	= 0;
	virtual QuorumVote*			NewVote()			const	= 0;
};

/*
===============================================================================

 QuroumVote

 An interface for abstracting the voting logic of the Paxos and PaxosLease
 replication algorithms.

===============================================================================
*/

class QuorumVote
{
public:
	virtual ~QuorumVote() {}
	
	virtual void				RegisterAccepted(uint64_t nodeID)	= 0;
	virtual void				RegisterRejected(uint64_t nodeID)	= 0;
	virtual void				Reset()								= 0;

	virtual bool				IsRejected() const				= 0;
	virtual bool				IsAccepted() const				= 0;
	virtual bool				IsComplete() const				= 0;
};

#endif
