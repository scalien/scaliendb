#ifndef DATACHUNKCONTEXT_H
#define DATACHUNKCONTEXT_H

#include "Framework/Replication/Quorums/QuorumContext.h"
#include "Framework/Replication/Quorums/DoubleQuorum.h"
#include "Framework/Replication/ReplicatedLog/ReplicatedLog.h"
#include "Framework/Replication/PaxosLease/PaxosLeaseLearner.h"
/*
===============================================================================

 DataChunkContext

===============================================================================
*/

class DataChunkContext : public QuorumContext
{
	DataChunkContext();
	
	void							SetContextID(uint64_t contextID);
	
	// implementation of QuorumContext interface:
	virtual bool					IsLeaderKnown();
	virtual bool					IsLeader();
	virtual uint64_t				GetLeader();
	virtual uint64_t				GetContextID() const;

	virtual uint64_t				GetPaxosID() const;
	virtual void					SetPaxosID(uint64_t paxosID);
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

	DoubleQuorum					quorum;
	QuorumDatabase					database;
	QuorumTransport					transport;

	ReplicatedLog					replicatedLog;
	PaxosLeaseLearner				paxosLeaseLearner;

	uint64_t						contextID;
	uint64_t						highestPaxosID;
};

#endif
