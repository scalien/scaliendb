#include "DataChunkContext.h"
#include "ReplicationManager.h"
#include "Framework/Replication/PaxosLease/PaxosLeaseMessage.h"
#include "Framework/Replication/Paxos/PaxosMessage.h"
#include "Application/Controller/ClusterMessage.h"

void DataChunkContext::Start()
{
	replicatedLog.Init(this);
	paxosLease.Init(this);
	highestPaxosID = 0;
}

void DataChunkContext::SetContextID(uint64_t contextID_)
{
	contextID = contextID_;
}

void DataChunkContext::SetDatabase(QuorumDatabase& database_)
{
	database = database_;
}

void DataChunkContext::SetTransport(QuorumTransport& transport_)
{
	transport = transport_;
}

bool DataChunkContext::IsLeaderKnown()
{
	return paxosLease.IsLeaseKnown();
}

bool DataChunkContext::IsLeader()
{
	return paxosLease.IsLeaseOwner();
}

uint64_t DataChunkContext::GetLeader()
{
	return paxosLease.GetLeaseOwner();
}

uint64_t DataChunkContext::GetContextID() const
{
	return contextID;
}

void DataChunkContext::OnLearnLease()
{
	replicatedLog.OnLearnLease();
}

void DataChunkContext::OnLeaseTimeout()
{
	replicatedLog.OnLeaseTimeout();
}


uint64_t DataChunkContext::GetPaxosID() const
{
	return replicatedLog.GetPaxosID();
}

void DataChunkContext::SetPaxosID(uint64_t paxosID)
{
	replicatedLog.SetPaxosID(paxosID);
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

void DataChunkContext::OnAppend(ReadBuffer value)
{
	// TODO
}

Buffer* DataChunkContext::GetNextValue()
{
	return NULL;
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
	
	switch(proto)
	{
		case 'L':
			OnPaxosLeaseMessage(buffer);
			break;
		case 'P':
			OnPaxosMessage(buffer);
			break;
		case 'C':
			OnClusterMessage(buffer);
			break;
		default:
			ASSERT_FAIL();
			break;
	}
}

void DataChunkContext::OnPaxosLeaseMessage(ReadBuffer buffer)
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

void DataChunkContext::OnPaxosMessage(ReadBuffer buffer)
{
	PaxosMessage msg;
	
	msg.Read(buffer);
	RegisterPaxosID(msg.paxosID);
	replicatedLog.RegisterPaxosID(msg.paxosID, msg.nodeID);
	replicatedLog.OnMessage(msg);
}

void DataChunkContext::OnClusterMessage(ReadBuffer buffer)
{
	ClusterMessage msg;
		
	msg.Read(buffer);
	for (unsigned i = 0; i < msg.numNodes; i++)
	{
		Log_Trace("nodeID = %" PRIu64 ", endpoint = %s", 
		 msg.nodeIDs[i], msg.endpoints[i].ToString());
		RMAN->GetTransport()->AddEndpoint(msg.nodeIDs[i], msg.endpoints[i]);
		quorum.AddNode(msg.nodeIDs[i]);
	}
	
	paxosLease.AcquireLease();
}

void DataChunkContext::RegisterPaxosID(uint64_t paxosID)
{
	if (paxosID > highestPaxosID)
		highestPaxosID = paxosID;
}
