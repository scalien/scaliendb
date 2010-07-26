#include "DataChunkContext.h"
#include "ReplicationManager.h"
#include "Framework/Replication/PaxosLease/PaxosLeaseMessage.h"
#include "Framework/Replication/Paxos/PaxosMessage.h"

DataChunkContext::DataChunkContext()
{
	replicatedLog.Init(this);
	paxosLeaseLearner.Init(this);
	highestPaxosID = 0;
}

void DataChunkContext::SetLogID(uint64_t logID_)
{
	logID = logID_;
}

bool DataChunkContext::IsLeaderKnown()
{
	return paxosLeaseLearner.IsLeaseKnown();
}

bool DataChunkContext::IsLeader()
{
	return paxosLeaseLearner.IsLeaseOwner();
}

uint64_t DataChunkContext::GetLeader()
{
	return paxosLeaseLearner.IsLeaseOwner();
}

uint64_t DataChunkContext::GetLogID() const
{
	return logID;
}

uint64_t DataChunkContext::GetPaxosID() const
{
	return replicatedLog.GetPaxosID();
}

uint64_t DataChunkContext::GetHighestPaxosID() const
{
	return highestPaxosID;
}

Quorum* DataChunkContext::GetQuorum()
{
	return &quorum;
}

QuorumDatabase*	DataChunkContext::GetDatabase()
{
	return &database;
}

QuorumTransport* DataChunkContext::GetTransport()
{
	return &transport;
}

void DataChunkContext::OnMessage()
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

void DataChunkContext::OnPaxosLeaseMessage(ReadBuffer buffer)
{
	PaxosLeaseMessage msg;
	
	msg.Read(buffer);
	assert(msg.type == PAXOSLEASE_LEARN_CHOSEN);
	RegisterPaxosID(msg.paxosID);
	replicatedLog.RegisterPaxosID(msg.paxosID, msg.nodeID);
	paxosLeaseLearner.OnMessage(msg);
	// TODO: right now PaxosLeaseLearner will not call back
}

void DataChunkContext::OnPaxosMessage(ReadBuffer buffer)
{
	PaxosMessage msg;
	
	msg.Read(buffer);
	RegisterPaxosID(msg.paxosID);
	replicatedLog.RegisterPaxosID(msg.paxosID, msg.nodeID);
	replicatedLog.OnMessage(msg);
}

void DataChunkContext::RegisterPaxosID(uint64_t paxosID)
{
	if (paxosID > highestPaxosID)
		highestPaxosID = paxosID;
}
