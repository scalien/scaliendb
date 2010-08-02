#include "ControlChunkContext.h"
#include "ConfigMessage.h"
#include "Controller.h"

void ControlChunkContext::Start()
{
	replicatedLog.Init(this);
	highestPaxosID = 0;
}

void ControlChunkContext::SetController(Controller* controller_)
{
	controller = controller_;
}

void ControlChunkContext::SetContextID(uint64_t contextID_)
{
	contextID = contextID_;
}

void ControlChunkContext::SetQuorum(DoubleQuorum& quorum_)
{
	quorum = quorum_;
}

void ControlChunkContext::SetDatabase(QuorumDatabase& database_)
{
	database = database_;
}

void ControlChunkContext::SetTransport(QuorumTransport& transport_)
{
	transport = transport_;
}

bool ControlChunkContext::IsLeaderKnown()
{
	// TODO: get from ControlDB
}

bool ControlChunkContext::IsLeader()
{
	return false;
}

uint64_t ControlChunkContext::GetLeader()
{
	// TODO: get from ControlDB
}

void ControlChunkContext::OnLearnLease()
{
	replicatedLog.OnLearnLease();
}

void ControlChunkContext::OnLeaseTimeout()
{
	replicatedLog.OnLeaseTimeout();
}

uint64_t ControlChunkContext::GetContextID() const
{
	return contextID;
}

uint64_t ControlChunkContext::GetPaxosID() const
{
	return replicatedLog.GetPaxosID();
}

void ControlChunkContext::SetPaxosID(uint64_t paxosID)
{
	replicatedLog.SetPaxosID(paxosID);
}

uint64_t ControlChunkContext::GetHighestPaxosID() const
{
	return highestPaxosID;
}

Quorum* ControlChunkContext::GetQuorum()
{
	return &quorum;
}

QuorumDatabase*	ControlChunkContext::GetDatabase()
{
	return &database;
}

QuorumTransport* ControlChunkContext::GetTransport()
{
	return &transport;
}

void ControlChunkContext::OnAppend(ReadBuffer value)
{
	ConfigMessage msg;
	msg.Read(value);
	controller->OnConfigMessage(msg);

	nextValue.Clear();	
}

Buffer* ControlChunkContext::GetNextValue()
{
	if (nextValue.GetLength() > 0)
		return &nextValue;
	
	return NULL;	
}

void ControlChunkContext::OnMessage()
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
		case 'P':
			OnPaxosMessage(buffer);
			break;
		default:
			ASSERT_FAIL();
			break;
	}
}

void ControlChunkContext::OnPaxosMessage(ReadBuffer buffer)
{
	PaxosMessage msg;
	
	msg.Read(buffer);
	RegisterPaxosID(msg.paxosID);
	replicatedLog.RegisterPaxosID(msg.paxosID, msg.nodeID);
	replicatedLog.OnMessage(msg);
}

void ControlChunkContext::RegisterPaxosID(uint64_t paxosID)
{
	if (paxosID > highestPaxosID)
		highestPaxosID = paxosID;
}