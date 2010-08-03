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
		key.Writef("/chunkID:1/nodeID:%d", i);
		key.NullTerminate();
		table->Get(NULL, key.GetBuffer(), nodeIDs[i]);
		key.Writef("/chunkID:1/endpoint:%d", i);
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
	
	table->Set(NULL, "/chunkID:1/numNodes", msg.numNodes);
	
	for (int i = 0; i < numNodes; i++)
	{
		key.Writef("/chunkID:1/nodeID:%d", i);
		key.NullTerminate();
		table->Set(NULL, key.GetBuffer(), msg.nodeIDs[i]);
		key.Writef("/chunkID:1/endpoint:%d", i);
		key.NullTerminate();
		endpoint = msg.endpoints[i];
		value.Write(endpoint.ToString());
		table->Set(NULL, key, value);
	}
}

bool Controller::HandleRequest(HttpConn* conn, const HttpRequest& request)
{
	Buffer			buffer;
	QuorumContext*	context;
	
	context = RMAN->GetContext(0);
	
	if (context->IsLeaderKnown())
	{
		buffer.Writef("ChunkID: 0\nMaster: %U\nSelf: %U\nPaxosID: %U\n", 
		 context->GetLeader(),
		 RMAN->GetNodeID(),
		 context->GetPaxosID());
	}
	else
	{
		buffer.Writef("ChunkID: 0\nNo master, PaxosID: %U\n", context->GetPaxosID());
	}

	conn->Write(buffer.GetBuffer(), buffer.GetLength());
	conn->Flush(true);
	
	return true;
}

void Controller::OnIncomingConnectionReady(uint64_t nodeID, Endpoint endpoint)
{
	ControlConfigContext*	context;
	
	Log_Trace();
	
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
		Log_Trace("got 3 nodes");
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
		context = (ControlConfigContext*) RMAN->GetContext(0);
		context->Append(msg);
	}
}

void Controller::OnConfigMessage(const ConfigMessage& msg)
{
	ClusterMessage		omsg;

	Log_Trace();
	
	assert(msg.type == CONFIG_CREATE_CHUNK);
	assert(msg.chunkID == 1);

	if (chunkCreated)
		return;

	WriteChunkQuorum(msg);
	chunkCreated = true;
	
	if (!RMAN->GetContext(0)->IsLeader())
		return;
	
	// send CLUSTER_INFO, first is the primary
	Log_Trace("Sending CLUSTER_INFO");
	omsg.type = CLUSTER_INFO;
	omsg.chunkID = msg.chunkID;
	omsg.numNodes = msg.numNodes;
	for (unsigned i = 0; i < msg.numNodes; i++)
	{
		omsg.nodeIDs[i] = msg.nodeIDs[i];
		omsg.endpoints[i] = msg.endpoints[i];
	}

	// send omsg to data nodes
	Buffer prefix;
	prefix.Writef("%U", msg.chunkID);
	for (unsigned i = 0; i < msg.numNodes; i++)
		RMAN->GetTransport()->SendPriorityMessage(msg.nodeIDs[i], prefix, omsg);
}
