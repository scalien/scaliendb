#include "ControlConfigContext.h"
#include "ReplicationManager.h"
#include "Framework/Replication/PaxosLease/PaxosLeaseMessage.h"
#include "Framework/Replication/Paxos/PaxosMessage.h"
#include "ClusterMessage.h"
#include "Controller.h"

void ControlConfigContext::Start()
{
	replicatedLog.Init(this);
	paxosLease.Init(this);
	highestPaxosID = 0;
	
	paxosLease.AcquireLease();
}

void ControlConfigContext::Append(ConfigMessage msg)
{
	Buffer buffer;
	msg.Write(nextValue);

	replicatedLog.TryAppendNextValue();
}

void ControlConfigContext::SetController(Controller* controller_)
{
	controller = controller_;
}

void ControlConfigContext::SetContextID(uint64_t contextID_)
{
	contextID = contextID_;
}

void ControlConfigContext::SetQuorum(SingleQuorum& quorum_)
{
	quorum = quorum_;
}

void ControlConfigContext::SetDatabase(QuorumDatabase& database_)
{
	database = database_;
}

void ControlConfigContext::SetTransport(QuorumTransport& transport_)
{
	transport = transport_;
}

bool ControlConfigContext::IsLeaderKnown()
{
	return paxosLease.IsLeaseKnown();
}

bool ControlConfigContext::IsLeader()
{
	return paxosLease.IsLeaseOwner();
}

uint64_t ControlConfigContext::GetLeader()
{
	return paxosLease.GetLeaseOwner();
}

void ControlConfigContext::OnLearnLease()
{
	replicatedLog.OnLearnLease();
}

void ControlConfigContext::OnLeaseTimeout()
{
	replicatedLog.OnLeaseTimeout();
}

uint64_t ControlConfigContext::GetContextID() const
{
	return contextID;
}

void ControlConfigContext::SetPaxosID(uint64_t paxosID)
{
	replicatedLog.SetPaxosID(paxosID);
}

uint64_t ControlConfigContext::GetPaxosID() const
{
	return replicatedLog.GetPaxosID();
}

uint64_t ControlConfigContext::GetHighestPaxosID() const
{
	return highestPaxosID;
}

Quorum* ControlConfigContext::GetQuorum()
{
	return &quorum;
}

QuorumDatabase*	ControlConfigContext::GetDatabase()
{
	return &database;
}

QuorumTransport* ControlConfigContext::GetTransport()
{
	return &transport;
}

void ControlConfigContext::OnAppend(ReadBuffer value, bool /*ownAppend*/)
{
	ConfigMessage msg;
	msg.Read(value);
	controller->OnConfigMessage(msg);

	nextValue.Clear();
}

Buffer* ControlConfigContext::GetNextValue()
{
	if (nextValue.GetLength() > 0)
		return &nextValue;
	
	return NULL;
}

void ControlConfigContext::OnMessage()
{
	char proto;
	ReadBuffer buffer;
	
	buffer = transport.GetMessage();
	
	Log_Trace("%.*s", P(&buffer));
	
	if (buffer.GetLength() < 2)
		ASSERT_FAIL();

	proto = buffer.GetCharAt(0);
	assert(buffer.GetCharAt(1) == ':');
	
	switch(proto)
	{
		case CLUSTER_PROTOCOL_ID:			//'C':
			OnClusterMessage(buffer);
			break;
		case PAXOSLEASE_PROTOCOL_ID:		//'L':
			OnPaxosLeaseMessage(buffer);
			break;
		case PAXOS_PROTOCOL_ID:				//'P':
			OnPaxosMessage(buffer);
			break;
		default:
			ASSERT_FAIL();
			break;
	}
}

void ControlConfigContext::OnPaxosLeaseMessage(ReadBuffer buffer)
{
	PaxosLeaseMessage msg;

	Log_Trace("%.*s", P(&buffer));
	
	msg.Read(buffer);
	if (msg.type == PAXOSLEASE_PREPARE_REQUEST || msg.type == PAXOSLEASE_LEARN_CHOSEN)
	{
		RegisterPaxosID(msg.paxosID);
		replicatedLog.RegisterPaxosID(msg.paxosID, msg.nodeID);
	}
	paxosLease.OnMessage(msg);
	// TODO: right now PaxosLeaseLearner will not call back
}

void ControlConfigContext::OnPaxosMessage(ReadBuffer buffer)
{
	PaxosMessage msg;
	
	msg.Read(buffer);
	RegisterPaxosID(msg.paxosID);
	replicatedLog.RegisterPaxosID(msg.paxosID, msg.nodeID);
	replicatedLog.OnMessage(msg);
}

void ControlConfigContext::OnClusterMessage(ReadBuffer buffer)
{
//	ClusterMessage msg;
//	
//	msg.Read(buffer);
//	
//	controller->OnClusterMessage(msg);
}

void ControlConfigContext::RegisterPaxosID(uint64_t paxosID)
{
	if (paxosID > highestPaxosID)
		highestPaxosID = paxosID;
}
