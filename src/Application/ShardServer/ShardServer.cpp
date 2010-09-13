#include "ShardServer.h"
#include "Application/Common/ContextTransport.h"
#include "Framework/Replication/ReplicationConfig.h"

void ShardServer::Init()
{
	unsigned		i;
	uint64_t		nodeID;
	const char*		str;
	Endpoint		endpoint;

	if (REPLICATION_CONFIG->GetNodeID() == 0)
		awaitingNodeID = true;
	else
		awaitingNodeID = false;
		
	// set my endpoint
	str = configFile.GetValue("endpoint", "");
	if (str == 0)
		ASSERT_FAIL();
	endpoint.Set(str);
	CONTEXT_TRANSPORT->Init(endpoint);
	
	if (REPLICATION_CONFIG->GetNodeID() > 0)
		CONTEXT_TRANSPORT->SetSelfNodeID(REPLICATION_CONFIG->GetNodeID());
	
	// connect to the controller nodes
	for (nodeID = 0; nodeID < (unsigned) configFile.GetListNum("controllers"); nodeID++)
	{
		str = configFile.GetListValue("controllers", i, "");
		endpoint.Set(str);
		CONTEXT_TRANSPORT->AddNode(i, endpoint);
		// this will cause the node to connect to the controllers
		// and if my nodeID is not set the MASTER will automatically send
		// me a SetNodeID cluster message
	}

}

void ShardServer::OnClusterMessage(uint64_t /*nodeID*/, ClusterMessage& msg)
{
	switch (msg.type)
	{
		case CLUSTER_SET_NODEID:
			if (!awaitingNodeID)
				return;
			CONTEXT_TRANSPORT->SetSelfNodeID(msg.nodeID);
			REPLICATION_CONFIG->SetNodeID(msg.nodeID);
			REPLICATION_CONFIG->Commit();
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
