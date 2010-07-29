#include "ControllerChunkContext.h"

void ControllerChunkContext::Init()
{
	replicatedLog.Init(this);
	highestPaxosID = 0;
}

void ControllerChunkContext::SetContextID(uint64_t contextID_)
{
	contextID = contextID_;
}

bool ControllerChunkContext::IsLeaderKnown()
{
	// TODO: get from ControlDB
}

bool ControllerChunkContext::IsLeader()
{
	return false;
}

uint64_t ControllerChunkContext::GetLeader()
{
	// TODO: get from ControlDB
}

uint64_t ControllerChunkContext::GetContextID() const
{
	return contextID;
}

uint64_t ControllerChunkContext::GetPaxosID() const
{
	return replicatedLog.GetPaxosID();
}

uint64_t ControllerChunkContext::GetHighestPaxosID() const
{
	return highestPaxosID;
}

Quorum* ControllerChunkContext::GetQuorum()
{
	return &quorum;
}

QuorumDatabase*	ControllerChunkContext::GetDatabase()
{
	return &database;
}

QuorumTransport* ControllerChunkContext::GetTransport()
{
	return &transport;
}

Buffer* ControllerChunkContext::GetNextValue()
{
	// TODO
}

void ControllerChunkContext::OnMessage()
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

void ControllerChunkContext::OnPaxosMessage(ReadBuffer buffer)
{
	PaxosMessage msg;
	
	msg.Read(buffer);
	RegisterPaxosID(msg.paxosID);
	replicatedLog.RegisterPaxosID(msg.paxosID, msg.nodeID);
	replicatedLog.OnMessage(msg);
}

void ControllerChunkContext::RegisterPaxosID(uint64_t paxosID)
{
	if (paxosID > highestPaxosID)
		highestPaxosID = paxosID;
}