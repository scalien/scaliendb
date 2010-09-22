#include "ConfigContext.h"
#include "Framework/Replication/ReplicationConfig.h"
#include "Framework/Replication/PaxosLease/PaxosLeaseMessage.h"
#include "Framework/Replication/Paxos/PaxosMessage.h"
#include "Controller.h"

void ConfigContext::Init(Controller* controller_, unsigned numControllers)
{
	uint64_t nodeID;
	
	controller = controller_;
	
	contextID = 0;
	for (nodeID = 0; nodeID < numControllers; nodeID++)
		quorum.AddNode(nodeID);

//	transport.SetPriority(); // TODO
	transport.SetQuorum(&quorum);
	transport.SetContextID(contextID);
	
	
	replicatedLog.Init(this);
	paxosLease.Init(this);
	transport.SetContextID(0);
	highestPaxosID = 0;	

	paxosLease.AcquireLease();
}

void ConfigContext::Append(ConfigMessage* message)
{
	message->Write(nextValue);

	replicatedLog.TryAppendNextValue();
}

bool ConfigContext::IsAppending()
{
	return (nextValue.GetLength() != 0);
}

bool ConfigContext::IsLeaderKnown()
{
	return paxosLease.IsLeaseKnown();
}

bool ConfigContext::IsLeader()
{
	return paxosLease.IsLeaseOwner();
}

uint64_t ConfigContext::GetLeader()
{
	return paxosLease.GetLeaseOwner();
}

void ConfigContext::OnLearnLease()
{
	replicatedLog.OnLearnLease();
	
	controller->OnLearnLease();
}

void ConfigContext::OnLeaseTimeout()
{
	nextValue.Clear();
	replicatedLog.OnLeaseTimeout();
}

uint64_t ConfigContext::GetContextID()
{
	return contextID;
}

void ConfigContext::SetPaxosID(uint64_t paxosID)
{
	replicatedLog.SetPaxosID(paxosID);
}

uint64_t ConfigContext::GetPaxosID()
{
	return replicatedLog.GetPaxosID();
}

uint64_t ConfigContext::GetHighestPaxosID()
{
	return highestPaxosID;
}

Quorum* ConfigContext::GetQuorum()
{
	return &quorum;
}

QuorumDatabase*	ConfigContext::GetDatabase()
{
	return &database;
}

QuorumTransport* ConfigContext::GetTransport()
{
	return &transport;
}

void ConfigContext::OnAppend(ReadBuffer value, bool ownAppend)
{
	ConfigMessage message;

	nextValue.Clear();

	assert(message.Read(value));
	controller->OnAppend(message, ownAppend);
}

Buffer* ConfigContext::GetNextValue()
{
	if (nextValue.GetLength() > 0)
		return &nextValue;
	
	return NULL;
}

void ConfigContext::OnMessage(ReadBuffer buffer)
{
	char proto;
	
	Log_Trace("%.*s", P(&buffer));
	
	if (buffer.GetLength() < 2)
		ASSERT_FAIL();

	proto = buffer.GetCharAt(0);
	assert(buffer.GetCharAt(1) == ':');
	
	switch(proto)
	{
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

void ConfigContext::OnPaxosLeaseMessage(ReadBuffer buffer)
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

void ConfigContext::OnPaxosMessage(ReadBuffer buffer)
{
	PaxosMessage msg;
	
	msg.Read(buffer);
	RegisterPaxosID(msg.paxosID);
	replicatedLog.RegisterPaxosID(msg.paxosID, msg.nodeID);
	replicatedLog.OnMessage(msg);
}

void ConfigContext::RegisterPaxosID(uint64_t paxosID)
{
	if (paxosID > highestPaxosID)
		highestPaxosID = paxosID;
}
