#include "Controller.h"
#include "Framework/Replication/ReplicationManager.h"
#include "Framework/Replication/Quorums/DoubleQuorum.h"

void Controller::Init(Table* table_)
{
	numNodes = 0;
	chunkCreated = false;
	table = table_;

	clusterInfoTimeout.SetCallable(MFUNC(Controller, OnSendClusterInfo));
	clusterInfoTimeout.SetDelay(1000);
	EventLoop::Add(&clusterInfoTimeout);
	
	ReadChunkQuorum(1);
}

void Controller::ReadChunkQuorum(uint64_t chunkID)
{
	Buffer key, value;

	Log_Trace();

	chunkCreated = false;

	key.Writef("#/chunkID:%U/numNodes", chunkID);
	key.NullTerminate();
	if (!table->Get(NULL, key.GetBuffer(), numNodes))
		return;
	
	for (unsigned i = 0; i < numNodes; i++)
	{
		key.Writef("#/chunkID:%U/nodeID:%d", chunkID, i);
		key.NullTerminate();
		table->Get(NULL, key.GetBuffer(), nodeIDs[i]);
		key.Writef("#/chunkID:%U/endpoint:%d", chunkID, i);
		key.NullTerminate();
		table->Get(NULL, key, value);
		value.NullTerminate();
		endpoints[i].Set(value.GetBuffer());
	}

	chunkCreated = true;
}

void Controller::WriteChunkQuorum(ConfigMessage& msg)
{
	Buffer key, value;
	Endpoint endpoint;

	Log_Trace();
	
	key.Writef("#/chunkID:%U/numNodes", msg.chunkID);
	key.NullTerminate();
	table->Set(NULL, key.GetBuffer(), msg.numNodes);
	
	for (unsigned i = 0; i < numNodes; i++)
	{
		key.Writef("#/chunkID:%U/nodeID:%d", msg.chunkID, i);
		key.NullTerminate();
		table->Set(NULL, key.GetBuffer(), msg.nodeIDs[i]);
		key.Writef("#/chunkID:%U/endpoint:%d", msg.chunkID, i);
		key.NullTerminate();
		endpoint = msg.endpoints[i];
		value.Write(endpoint.ToString());
		table->Set(NULL, key, value);
	}
}

bool Controller::HandleRequest(HttpConn* conn, HttpRequest& /*request*/)
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
		return;
	
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

void Controller::OnConfigMessage(ConfigMessage& msg)
{
	Buffer				value;
	Buffer				key;

	Log_Trace();
	
	assert(msg.type == CONFIG_CREATE_CHUNK);
	assert(msg.chunkID == 1);

	if (chunkCreated)
		return;

	WriteChunkQuorum(msg);
	chunkCreated = true;
}

void Controller::OnSendClusterInfo()
{
	ClusterMessage		omsg;
	uint64_t			chunkID;

	EventLoop::Add(&clusterInfoTimeout);

	if (!chunkCreated)
		return;

	if (!RMAN->GetContext(0)->IsLeader())
		return;
	
	Log_Trace();

	chunkID = 1; // TODO: hack
	
	// send CLUSTER_INFO, first is the primary
	Log_Trace("Sending CLUSTER_INFO");
	omsg.type = CLUSTER_INFO;
	omsg.chunkID = chunkID;
	omsg.numNodes = numNodes;
	for (unsigned i = 0; i < numNodes; i++)
	{
		omsg.nodeIDs[i] = nodeIDs[i];
		omsg.endpoints[i] = endpoints[i];
	}
	// send omsg to data nodes
	Buffer prefix;
	prefix.Writef("%U", chunkID);
	for (unsigned i = 0; i < numNodes; i++)
		RMAN->GetTransport()->SendPriorityMessage(nodeIDs[i], prefix, omsg);
}
