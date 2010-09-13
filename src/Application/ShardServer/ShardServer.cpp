#include "ShardServer.h"
#include "Application/Common/ContextTransport.h"
#include "Framework/Replication/ReplicationConfig.h"

void ShardServer::Init()
{
	unsigned		numControllers;
	uint64_t		nodeID;
	const char*		str;
	Endpoint		endpoint;

	if (REPLICATION_CONFIG->GetNodeID() == 0)
		awaitingNodeID = true;
	else
		awaitingNodeID = false;
	
	if (REPLICATION_CONFIG->GetNodeID() > 0)
		CONTEXT_TRANSPORT->SetSelfNodeID(REPLICATION_CONFIG->GetNodeID());
	
	// connect to the controller nodes
	numControllers = (unsigned) configFile.GetListNum("controllers");
	for (nodeID = 0; nodeID < numControllers; nodeID++)
	{
		str = configFile.GetListValue("controllers", nodeID, "");
		endpoint.Set(str);
		CONTEXT_TRANSPORT->AddNode(nodeID, endpoint);
		// this will cause the node to connect to the controllers
		// and if my nodeID is not set the MASTER will automatically send
		// me a SetNodeID cluster message
	}

	CONTEXT_TRANSPORT->SetClusterContext(this);
}

void ShardServer::OnClusterMessage(uint64_t /*nodeID*/, ClusterMessage& msg)
{
	Log_Trace();
	
	switch (msg.type)
	{
		case CLUSTER_SET_NODEID:
			if (!awaitingNodeID)
				return;
			awaitingNodeID = false;
			CONTEXT_TRANSPORT->SetSelfNodeID(msg.nodeID);
			REPLICATION_CONFIG->SetNodeID(msg.nodeID);
			REPLICATION_CONFIG->Commit();
			Log_Trace("my nodeID is %" PRIu64 "", msg.nodeID);
			break;
	}
}

void ShardServer::OnIncomingConnectionReady(uint64_t /*nodeID*/, Endpoint /*endpoint*/)
{
	// nothing
}

void ShardServer::OnAwaitingNodeID(Endpoint /*endpoint*/)
{
	// nothing
}
