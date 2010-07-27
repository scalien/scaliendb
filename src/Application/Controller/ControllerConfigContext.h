#ifndef CONTROLLERCONFIGCONTEXT_H
#define CONTROLLERCONFIGCONTEXT_H

#include "Framework/Replication/Quorums/QuorumContext.h"
#include "Framework/Replication/Quorums/SingleQuorum.h"
#include "Framework/Replication/ReplicatedLog/ReplicatedLog.h"
#include "Framework/Replication/PaxosLease/PaxosLease.h"

/*
===============================================================================

 ControllerConfigContext

===============================================================================
*/

class ControllerConfigContext : public QuorumContext
{
	void							Init();
	
	void							SetContextID(uint64_t contextID);
	
	// implementation of QuorumContext interface:
	virtual bool					IsLeaderKnown();
	virtual bool					IsLeader();
	virtual uint64_t				GetLeader();

	virtual void					OnLearnLease();
	virtual void					OnLeaseTimeout();

	virtual uint64_t				GetContextID() const;
	virtual uint64_t				GetPaxosID() const;
	virtual uint64_t				GetHighestPaxosID() const;
	
	virtual Quorum*					GetQuorum();
	virtual QuorumDatabase*			GetDatabase();
	virtual QuorumTransport*		GetTransport();
	
	virtual Buffer*					GetNextValue();
	virtual void					OnMessage();

private:
	void							OnPaxosLeaseMessage(ReadBuffer buffer);
	void							OnPaxosMessage(ReadBuffer buffer);
	void							RegisterPaxosID(uint64_t paxosID);

	SingleQuorum					quorum;
	QuorumDatabase					database;
	QuorumTransport					transport;

	ReplicatedLog					replicatedLog;
	PaxosLease						paxosLease;

	uint64_t						contextID;
	uint64_t						highestPaxosID;
};

#endif
