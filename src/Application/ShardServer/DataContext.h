#ifndef DATACONTEXT_H
#define DATACONTEXT_H

#include "Framework/Replication/Quorums/QuorumContext.h"
#include "Framework/Replication/Quorums/TotalQuorum.h"
#include "Framework/Replication/ReplicatedLog/ReplicatedLog.h"
#include "Framework/Replication/PaxosLease/PaxosLease.h"
#include "Application/Controller/State/ConfigQuorum.h"
#include "DataMessage.h"

class ShardServer; // forward

/*
===============================================================================================

 DataContext

===============================================================================================
*/

class DataContext : public QuorumContext
{
public:
	void							Init(ShardServer* shardServer, ConfigQuorum* quorum);
	
	void							Append(DataMessage* message);
	bool							IsAppending();
	
	// ========================================================================================
	// QuorumContext interface:
	//
	virtual bool					IsLeaderKnown();
	virtual bool					IsLeader();
	virtual uint64_t				GetLeader();

	virtual void					OnLearnLease();
	virtual void					OnLeaseTimeout();

	virtual uint64_t				GetContextID();
	virtual void					SetPaxosID(uint64_t paxosID);
	virtual uint64_t				GetPaxosID();
	virtual uint64_t				GetHighestPaxosID();
	
	virtual Quorum*				GetQuorum();
	virtual QuorumDatabase*		GetDatabase();
	virtual QuorumTransport*		GetTransport();
	
	virtual	void					OnAppend(ReadBuffer value, bool ownAppend);
	virtual Buffer*				GetNextValue();
	virtual void					OnMessage(ReadBuffer msg);
	// ========================================================================================

private:
	void							OnPaxosMessage(ReadBuffer buffer);
	void							RegisterPaxosID(uint64_t paxosID);

	ShardServer*					shardServer;
	TotalQuorum					quorum;
	QuorumDatabase				database;
	QuorumTransport				transport;
	ReplicatedLog					replicatedLog;
	uint64_t						contextID;
	uint64_t						highestPaxosID;
	Buffer						nextValue;
};

#endif
