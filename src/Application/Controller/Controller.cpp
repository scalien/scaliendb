#include "Controller.h"
#include "Framework/Replication/ReplicationManager.h"
#include "Framework/Replication/Quorums/DoubleQuorum.h"

void Controller::Init(Table* table_)
{
	numNodes = 0;
	chunkCreated = false;
	table = table_;
	
	if (!ReadChunkQuorum(1))
		return;

	chunkCreated = true;	
}

bool Controller::ReadChunkQuorum(uint64_t /*chunkID*/)
{
	Buffer key, value;
	
	if (!table->Get(NULL, "/chunkID:0/numNodes", numNodes))
		return false;
	
	for (int i = 0; i < numNodes; i++)
	{
		key.Writef("/chunkID:0/nodeID:%d", i);
		key.NullTerminate();
		table->Get(NULL, key.GetBuffer(), nodeIDs[i]);
		key.Writef("/chunkID:0/endpoint:%d", i);
		key.NullTerminate();
		table->Get(NULL, key, value);
		value.NullTerminate();
		endpoints[i].Set(value.GetBuffer());
	}
	
	return true;
}

void Controller::WriteChunkQuorum(const ConfigMessage& msg)
{
	Buffer key, value;
	Endpoint endpoint;
	
	table->Set(NULL, "/chunkID:0/numNodes", msg.numNodes);
	
	for (int i = 0; i < numNodes; i++)
	{
		key.Writef("/chunkID:0/nodeID:%d", i);
		key.NullTerminate();
		table->Set(NULL, key.GetBuffer(), msg.nodeIDs[i]);
		key.Writef("/chunkID:0/endpoint:%d", i);
		key.NullTerminate();
		endpoint = msg.endpoints[i];
		value.Write(endpoint.ToString());
		table->Set(NULL, key, value);
	}
}

bool Controller::HandleRequest(HttpConn* conn, const HttpRequest& request)
{
	Buffer buffer;
	
	if (configContext->IsLeaderKnown())
	{
		buffer.Writef("Master: %U\nSelf: %U\nPaxosID: %U\n", 
		 configContext->GetLeader(),
		 RMAN->GetNodeID(),
		 configContext->GetPaxosID());
	}
	else
	{
		buffer.Writef("No master, PaxosID: %U\n", configContext->GetPaxosID());
	}

	conn->Write(buffer.GetBuffer(), buffer.GetLength());
	conn->Flush(true);
	
	return true;
}

void Controller::SetConfigContext(ControlConfigContext* configContext_)
{
	configContext = configContext_;
}

void Controller::SetChunkContext(ControlChunkContext* chunkContext_)
{
	DoubleQuorum* quorum;

	chunkContext = chunkContext_;
	
	if (!chunkCreated)
		return;
	
	// set data nodes in this quorum
	quorum = (DoubleQuorum*) chunkContext->GetQuorum();
	for (unsigned i = 0; i < numNodes; i++)
		quorum->AddNode(QUORUM_DATA_NODE, nodeIDs[i]);
}

void Controller::OnIncomingConnectionReady(uint64_t nodeID, Endpoint endpoint)
{
	if (nodeID < 10) // TODO: data node
		return;
	
	if (chunkCreated)
	{
		// send CLUSTER_INFO, first is the primary
	}
	
	nodeIDs[numNodes] = nodeID;
	endpoints[numNodes] = endpoint;
	numNodes++;
	
	// TODO: a data node reconnecting is not handled
	
	if (numNodes == 3)
	{
		// replicate CONFIG_CREATE_CHUNK message with logID 1 and the 3 nodes
		ConfigMessage msg;
		msg.type = CONFIG_CREATE_CHUNK;
		msg.chunkID = 1;
		msg.numNodes = numNodes;
		for (unsigned i = 0; i < numNodes; i++)
		{
			msg.nodeIDs[i] = nodeIDs[i];
			msg.endpoints[i] = endpoints[i];
		}
		configContext->Append(msg);
	}
}

void Controller::OnConfigMessage(const ConfigMessage& msg)
{
	DoubleQuorum* quorum;
	
	assert(msg.type == CONFIG_CREATE_CHUNK);
	assert(msg.chunkID == 1);

	if (chunkCreated)
		return;

	WriteChunkQuorum(msg);
	chunkCreated = true;
	
	chunkContext = GetChunkContext(msg.chunkID);
	// set data nodes in this quorum
	quorum = (DoubleQuorum*) chunkContext->GetQuorum();
	for (unsigned i = 0; i < numNodes; i++)
		quorum->AddNode(QUORUM_DATA_NODE, nodeIDs[i]);
	// TODO: GetVote() issue
	
	// send CLUSTER_INFO, first is the primary
	Log_Trace("Should send CLUSTER_INFO");
}

ControlChunkContext* Controller::GetChunkContext(uint64_t chunkID)
{
	assert(chunkID == 1);
	
	return chunkContext;
}
