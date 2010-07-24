#include "DataNodeContext.h"
#include "ReplicationManager.h"
#include "Framework/Replication/PaxosLease/PaxosLeaseMessage.h"

DataNodeContext::DataNodeContext()
{
	replicatedLog.Init(this);
	paxosLeaseLearner.Init(this);
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
	
	paxosLeaseLearner.OnMessage(msg);	
}

void DataNodeContext::OnPaxosMessage(ReadBuffer buffer)
{
	replicatedLog.OnMessage(buffer);
}
