#include "ControllerConfigContext.h"
#include "ReplicationManager.h"
#include "Framework/Replication/PaxosLease/PaxosLeaseMessage.h"
#include "Framework/Replication/Paxos/PaxosMessage.h"

void ControllerConfigContext::Start()
{
	// TODO: configure quorum from static config file
	
	replicatedLog.Init(this);
	paxosLease.Init(this);
	highestPaxosID = 0;	
}

void ControllerConfigContext::SetContextID(uint64_t contextID_)
{
	contextID = contextID_;
}

void ControllerConfigContext::SetQuorum(SingleQuorum& quorum_)
{
	quorum = quorum_;
}

void ControllerConfigContext::SetDatabase(QuorumDatabase& database_)
{
	database = database_;
}

void ControllerConfigContext::SetTransport(QuorumTransport& transport_)
{
	transport = transport_;
}

bool ControllerConfigContext::IsLeaderKnown()
{
	return paxosLease.IsLeaseKnown();
}

bool ControllerConfigContext::IsLeader()
{
	return paxosLease.IsLeaseOwner();
}

uint64_t ControllerConfigContext::GetLeader()
{
	return paxosLease.IsLeaseOwner();
}

void ControllerConfigContext::OnLearnLease()
{
	replicatedLog.OnLearnLease();
}

void ControllerConfigContext::OnLeaseTimeout()
{
	replicatedLog.OnLeaseTimeout();
}

uint64_t ControllerConfigContext::GetContextID() const
{
	return contextID;
}

void ControllerConfigContext::SetPaxosID(uint64_t paxosID)
{
	replicatedLog.SetPaxosID(paxosID);
}

uint64_t ControllerConfigContext::GetPaxosID() const
{
	return replicatedLog.GetPaxosID();
}

uint64_t ControllerConfigContext::GetHighestPaxosID() const
{
	return highestPaxosID;
}

Quorum* ControllerConfigContext::GetQuorum()
{
	return &quorum;
}

QuorumDatabase*	ControllerConfigContext::GetDatabase()
{
	return &database;
}

QuorumTransport* ControllerConfigContext::GetTransport()
{
	return &transport;
}

Buffer* ControllerConfigContext::GetNextValue()
{
	// TODO
}

void ControllerConfigContext::OnMessage()
{
	char proto;
	ReadBuffer buffer;
	
	buffer = transport.GetMessage();
	
	Log_Trace("%.*s", P(&buffer));
	
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

void ControllerConfigContext::OnPaxosLeaseMessage(ReadBuffer buffer)
{
	PaxosLeaseMessage msg;

	Log_Trace("%.*s", P(&buffer));
	
	msg.Read(buffer);
	RegisterPaxosID(msg.paxosID);
	if (msg.type == PAXOSLEASE_PREPARE_REQUEST ||
	 msg.type == PAXOSLEASE_LEARN_CHOSEN)
		replicatedLog.RegisterPaxosID(msg.paxosID, msg.nodeID);
	paxosLease.OnMessage(msg);
	// TODO: right now PaxosLeaseLearner will not call back
}

void ControllerConfigContext::OnPaxosMessage(ReadBuffer buffer)
{
	PaxosMessage msg;
	
	msg.Read(buffer);
	RegisterPaxosID(msg.paxosID);
	replicatedLog.RegisterPaxosID(msg.paxosID, msg.nodeID);
	replicatedLog.OnMessage(msg);
}

void ControllerConfigContext::RegisterPaxosID(uint64_t paxosID)
{
	if (paxosID > highestPaxosID)
		highestPaxosID = paxosID;
}
