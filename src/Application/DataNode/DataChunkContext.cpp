#include "DataChunkContext.h"
#include "Framework/Replication/ReplicationConfig.h"
#include "Framework/Replication/PaxosLease/PaxosLeaseMessage.h"
#include "Framework/Replication/Paxos/PaxosMessage.h"
#include "Application/DataNode/DataNode.h"
#include "Application/Controller/ClusterMessage.h"

void DataChunkContext::Start(DataNode* dataNode_)
{
	dataNode = dataNode_;
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
//	database = database_;
//	table = database.table; // TODO: HACK
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

uint64_t DataChunkContext::GetContextID()
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


uint64_t DataChunkContext::GetPaxosID()
{
	return replicatedLog.GetPaxosID();
}

void DataChunkContext::SetPaxosID(uint64_t paxosID)
{
	replicatedLog.SetPaxosID(paxosID);
}

uint64_t DataChunkContext::GetHighestPaxosID()
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

void DataChunkContext::OnAppend(ReadBuffer value, bool ownAppend)
{
	DataMessage		msg;
	DataMessage*	msgptr;
	Log_Trace();
	
	msg.Read(value);
	Buffer bkey, bvalue;
	bkey.Write("$");
	bkey.Append(msg.key);
	bvalue.Write(msg.value);
	table->Set(NULL, bkey, bvalue);
	
	if (!ownAppend)
	{
		Log_Trace("not my command");
		return;
	}
	
	Log_Trace("my append");
	
	msgptr = messages.First();
	dataNode->GetService()->OnComplete(msgptr, true);
	messages.Remove(msgptr);
	delete msgptr;
}

Buffer* DataChunkContext::GetNextValue()
{
	DataMessage*	msg;

	msg = messages.First();
	
	if (!msg)
	{
		Log_Trace("no data message to be replicated");
		return NULL;
	}
	
	Log_Trace();
	
	msg->Write(nextValue);
	
	return &nextValue;
}

void DataChunkContext::OnMessage()
{
// TODO: commented out
//	char proto;
//	ReadBuffer buffer;
//	
//	buffer = transport.GetMessage();
//	
//	if (buffer.GetLength() < 2)
//		ASSERT_FAIL();
//
//	proto = buffer.GetCharAt(0);
//	assert(buffer.GetCharAt(1) == ':');
//	
//	switch(proto)
//	{
//		case 'L':
//			OnPaxosLeaseMessage(buffer);
//			break;
//		case 'P':
//			OnPaxosMessage(buffer);
//			break;
//		case 'C':
//			OnClusterMessage(buffer);
//			break;
//		default:
//			ASSERT_FAIL();
//			break;
//	}
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
		CONTEXT_TRANSPORT->AddEndpoint(msg.nodeIDs[i], msg.endpoints[i]);
		quorum.AddNode(msg.nodeIDs[i]);
	}
	
	paxosLease.AcquireLease();
}

void DataChunkContext::RegisterPaxosID(uint64_t paxosID)
{
	if (paxosID > highestPaxosID)
		highestPaxosID = paxosID;
}

void DataChunkContext::Get(ReadBuffer& key, void* ptr)
{
	DataMessage* msg;
	msg = new DataMessage;
	msg->Get(key);
	msg->ptr = ptr;

	if (!IsLeader())
	{
		dataNode->GetService()->OnComplete(msg, false);
		delete msg;
		return;
	}
	
	Buffer bkey, bvalue;
	bkey.Writef("$");
	bkey.Append(key);
	bool found = table->Get(NULL, bkey, bvalue);
	if (found)
		msg->value.Wrap(bvalue);
	dataNode->GetService()->OnComplete(msg, found);
	delete msg;
}

void DataChunkContext::Set(ReadBuffer& key, ReadBuffer& value, void* ptr)
{
	DataMessage* msg;
	msg = new DataMessage;
	msg->Set(key, value);
	msg->ptr = ptr;

	if (!IsLeader())
	{
		dataNode->GetService()->OnComplete(msg, false);
		delete msg;
		return;
	}

	messages.Append(msg);
	replicatedLog.TryAppendNextValue();
}
