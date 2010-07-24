#include "Quorums/QuorumContext.h"
#include "Quorums/DoubleQuorum.h"
#include "ReplicatedLog/ReplicatedLog.h"
#include "PaxosLease/PaxosLeaseLearner.h"
/*
===============================================================================

 DataNodeContext

===============================================================================
*/

class DataNodeContext : public QuorumContext
{
	DataNodeContext();
	
	void							SetLogID(uint64_t logID);
	
	// implementation of QuorumContext interface:
	virtual bool					IsLeaderKnown();
	virtual bool					IsLeader();
	virtual uint64_t				GetLeader();
	virtual uint64_t				GetLogID() const;
	virtual uint64_t				GetPaxosID() const;
	virtual uint64_t				GetHighestPaxosID() const;

	virtual Quorum*					GetQuorum() const;
	virtual QuorumDatabase*			GetDatabase() const;
	virtual QuorumTransport*		GetTransport() const;

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

	uint64_t						logID;
	uint64_t						highestPaxosID;
};

