#ifndef CONFIGCONTEXT_H
#define CONFIGCONTEXT_H

#include "Framework/Replication/Quorums/QuorumContext.h"
#include "Framework/Replication/Quorums/MajorityQuorum.h"
#include "Framework/Replication/ReplicatedLog/ReplicatedLog.h"
#include "Framework/Replication/PaxosLease/PaxosLease.h"
#include "ConfigMessage.h"

class Controller; // forward

/*
===============================================================================

 ConfigContext

===============================================================================
*/

class ConfigContext : public QuorumContext
{
public:
	void							Init(Controller* controller, unsigned numControllers);
	void							SetController(Controller* controller);
	
	void							Append(ConfigMessage message);
	bool							IsAppending();
	
	/* ---------------------------------------------------------------------------------------- */
	/* QuorumContext interface:																	*/
	/*																							*/
	virtual bool					IsLeaderKnown();
	virtual bool					IsLeader();
	virtual uint64_t				GetLeader();

	virtual void					OnLearnLease();
	virtual void					OnLeaseTimeout();

	virtual uint64_t				GetContextID();
	virtual void					SetPaxosID(uint64_t paxosID);
	virtual uint64_t				GetPaxosID();
	virtual uint64_t				GetHighestPaxosID();
	
	virtual Quorum*					GetQuorum();
	virtual QuorumDatabase*			GetDatabase();
	virtual QuorumTransport*		GetTransport();
	
	virtual	void					OnAppend(ReadBuffer value, bool ownAppend);
	virtual Buffer*					GetNextValue();
	virtual void					OnMessage(ReadBuffer msg);
	/* ---------------------------------------------------------------------------------------- */

private:
	void							OnPaxosLeaseMessage(ReadBuffer buffer);
	void							OnPaxosMessage(ReadBuffer buffer);
	void							RegisterPaxosID(uint64_t paxosID);

	Controller*						controller;
	MajorityQuorum					quorum;
	QuorumDatabase					database;
	QuorumTransport					transport;
	ReplicatedLog					replicatedLog;
	PaxosLease						paxosLease;
	uint64_t						contextID;
	uint64_t						highestPaxosID;
	Buffer							nextValue;
};

#endif
