#include "DataNodeContext.h"
#include "ReplicationManager.h"
#include "Framework/Replication/PaxosLease/PaxosLeaseMessage.h"
#include "Framework/Replication/Paxos/PaxosMessage.h"

DataNodeContext::DataNodeContext()
{
	replicatedLog.Init(this);
	paxosLeaseLearner.Init(this);
	highestPaxosID = 0;
}

void DataNodeContext::SetLogID(uint64_t logID_)
{
	logID = logID_;
}

bool DataNodeContext::IsLeaderKnown()
{
	return paxosLeaseLearner.IsLeaseKnown();
}

bool DataNodeContext::IsLeader()
{
	return paxosLeaseLearner.IsLeaseOwner();
}

uint64_t DataNodeContext::GetLeader()
{
	return paxosLeaseLearner.IsLeaseOwner();
}

uint64_t DataNodeContext::GetLogID() const
{
	return logID;
}

uint64_t DataNodeContext::GetPaxosID() const
{
	return replicatedLog.GetPaxosID();
}

uint64_t DataNodeContext::GetHighestPaxosID() const
{
	return highestPaxosID;
}

Quorum* DataNodeContext::GetQuorum()
{
	return &quorum;
}

QuorumDatabase*	DataNodeContext::GetDatabase()
{
	return &database;
}

QuorumTransport* DataNodeContext::GetTransport()
{
	return &transport;
}

void DataNodeContext::OnMessage()
{
	char proto;
	ReadBuffer buffer;
	
	buffer = transport.GetMessage();
	
	if (buffer.GetLength() < 2)
		ASSERT_FAIL();

	proto = buffer.GetCharAt(0);
	assert(buffer.GetCharAt(1) == ':');
	buffer.Advance(2);
	
	switch(proto)
	{
		case 'L':
			OnPaxosLeaseMessage(buffer);
			break;
		case 'P':
			OnPaxosMessage(buffer);
			break;
		default:
			ASSERT_FAIL();
			break;
	}
}

void DataNodeContext::OnPaxosLeaseMessage(ReadBuffer buffer)
{
	PaxosLeaseMessage msg;
	
	msg.Read(buffer);
	assert(msg.type == PAXOSLEASE_LEARN_CHOSEN);
	RegisterPaxosID(msg.paxosID);
	replicatedLog.RegisterPaxosID(msg.paxosID, msg.nodeID);
	paxosLeaseLearner.OnMessage(msg);
	// TODO: right now PaxosLeaseLearner will not call back
}

void DataNodeContext::OnPaxosMessage(ReadBuffer buffer)
{
	PaxosMessage msg;
	
	msg.Read(buffer);
	RegisterPaxosID(msg.paxosID);
	replicatedLog.RegisterPaxosID(msg.paxosID, msg.nodeID);
	replicatedLog.OnMessage(msg);
}

void DataNodeContext::RegisterPaxosID(uint64_t paxosID)
{
	if (paxosID > highestPaxosID)
		highestPaxosID = paxosID;
}
