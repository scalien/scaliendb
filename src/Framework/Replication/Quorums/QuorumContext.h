#ifndef QUORUMCONTEXT_H
#define QUORUMCONTEXT_H

#include "System/Events/Callable.h"
#include "Quorum.h"

#include "Framework/Replication/Quorums/QuorumDatabase.h"
#include "Framework/Replication/Quorums/QuorumTransport.h"

/*
===============================================================================

 QuorumContext
 
 This should be called QuorumContext.

===============================================================================
*/

class QuorumContext
{
public:
	virtual ~QuorumContext() {};

	virtual bool					IsLeader()										= 0;	
	virtual bool					IsLeaderKnown()									= 0;
	virtual uint64_t				GetLeader()										= 0;

	virtual void					OnLearnLease()									= 0;
	virtual void					OnLeaseTimeout()								= 0;

	virtual uint64_t				GetContextID()									= 0;
	virtual void					SetPaxosID(uint64_t paxosID)					= 0;
	virtual uint64_t				GetPaxosID()									= 0;
	virtual uint64_t				GetHighestPaxosID()								= 0;

	virtual Quorum*					GetQuorum()										= 0;
	virtual QuorumDatabase*			GetDatabase()									= 0;
	virtual QuorumTransport*		GetTransport()									= 0;
	
	virtual	void					OnAppend(ReadBuffer value, bool ownAppend)		= 0;
	virtual Buffer*					GetNextValue()									= 0;
	virtual void					OnMessage()										= 0;
};

#endif
