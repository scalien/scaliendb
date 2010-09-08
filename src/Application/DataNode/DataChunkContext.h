#ifndef DATACHUNKCONTEXT_H
#define DATACHUNKCONTEXT_H

#include "Framework/Replication/Quorums/QuorumContext.h"
#include "Framework/Replication/Quorums/SingleQuorum.h"
#include "Framework/Replication/ReplicatedLog/ReplicatedLog.h"
#include "Framework/Replication/PaxosLease/PaxosLease.h"
#include "Application/DataNode/DataMessage.h"

class DataNode; // forward

/*
===============================================================================

 DataChunkContext

===============================================================================
*/

class DataChunkContext : public QuorumContext
{
public:
	void							Start(DataNode* dataNode_);
	
	void							SetContextID(uint64_t contextID);
	void							SetDatabase(QuorumDatabase& database);
	void							SetTransport(QuorumTransport& transport);
	
	// implementation of QuorumContext interface:
	virtual bool					IsLeaderKnown();
	virtual bool					IsLeader();
	virtual uint64_t				GetLeader();
	virtual uint64_t				GetContextID();

	virtual void					OnLearnLease();
	virtual void					OnLeaseTimeout();
	
	virtual uint64_t				GetPaxosID();
	virtual void					SetPaxosID(uint64_t paxosID);
	virtual uint64_t				GetHighestPaxosID();

	virtual Quorum*					GetQuorum();
	virtual QuorumDatabase*			GetDatabase();
	virtual QuorumTransport*		GetTransport();

	virtual	void					OnAppend(ReadBuffer value, bool ownAppend);
	virtual Buffer*					GetNextValue();
	virtual void					OnMessage();
	
	void							Get(ReadBuffer& key, void* ptr);
	void							Set(ReadBuffer& key, ReadBuffer& value, void* ptr);

private:
	void							OnPaxosLeaseMessage(ReadBuffer buffer);
	void							OnPaxosMessage(ReadBuffer buffer);
	void							OnClusterMessage(ReadBuffer buffer);
	void							RegisterPaxosID(uint64_t paxosID);

	SingleQuorum					quorum;
	QuorumDatabase					database;
	QuorumTransport					transport;

	ReplicatedLog					replicatedLog;
	PaxosLease						paxosLease;

	uint64_t						contextID;
	uint64_t						highestPaxosID;
	
	InList<DataMessage>				messages;
	Buffer							nextValue;
//	Table*							table;
	DataNode*						dataNode;
};

#endif
