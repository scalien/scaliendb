#include "Controller.h"
#include "ConfigCommand.h"
#include "System/Config.h"
#include "Framework/Replication/ReplicationConfig.h"
#include "Application/Common/ContextTransport.h"
#include "Application/Common/ClientConnection.h"
#include "Application/Common/ClusterMessage.h"

void Controller::Init()
{
	unsigned		numControllers;
	int64_t			nodeID;
	const char*		str;
	Endpoint		endpoint;
	
	nodeID = configFile.GetIntValue("nodeID", -1);
	if (nodeID < 0)
		ASSERT_FAIL();
	
	CONTEXT_TRANSPORT->SetSelfNodeID(nodeID);
	REPLICATION_CONFIG->SetNodeID(nodeID);
	
	CONTEXT_TRANSPORT->SetClusterContext(this);
	
	InitConfigContext();
	
	//TODO:
	nextNodeID = 100;

	// connect to the controller nodes
	numControllers = (unsigned) configFile.GetListNum("controllers");
	for (nodeID = 0; nodeID < numControllers; nodeID++)
	{
		str = configFile.GetListValue("controllers", nodeID, "");
		endpoint.Set(str);
		CONTEXT_TRANSPORT->AddNode(nodeID, endpoint);
	}

	configContext.Init(this, numControllers);
	CONTEXT_TRANSPORT->AddQuorumContext(&configContext);
}

void Controller::InitConfigContext()
{
}

bool Controller::IsMasterKnown()
{
	return configContext.IsLeaderKnown();
}

bool Controller::IsMaster()
{
	return configContext.IsLeader();
}

uint64_t Controller::GetMaster()
{
	return configContext.GetLeader();
}

bool Controller::ProcessClientCommand(ClientConnection* /*conn*/, ConfigCommand& command)
{
	// TODO:
	// 1. verify all the IDs
	// 2. complete the command where necessary
	
	if (command.type == CONFIG_CREATE_TABLE)
	{	
		// TODO: assign a new shardID
	}
	return true;
}

void Controller::OnLearnLease()
{
	Endpoint endpoint;
	
	if (CONTEXT_TRANSPORT->GetNextWaiting(endpoint))
		TryRegisterShardServer(endpoint);
}

void Controller::OnConfigCommand(ConfigCommand& command)
{
	ClusterMessage clusterMessage;
	
	if (command.type == CONFIG_REGISTER_SHARDSERVER)
	{
		// tell ContextTransport that this connection has a new nodeID
		CONTEXT_TRANSPORT->SetConnectionNodeID(command.endpoint, command.nodeID);
		
		// tell the shard server
		clusterMessage.SetNodeID(command.nodeID);
		CONTEXT_TRANSPORT->SendClusterMessage(command.nodeID, clusterMessage);
	}
	
	
}

void Controller::OnClusterMessage(uint64_t /*nodeID*/, ClusterMessage& /*msg*/)
{
	if (!IsMaster())
		return;	
}

void Controller::OnIncomingConnectionReady(uint64_t /*nodeID*/, Endpoint /*endpoint*/)
{
}

void Controller::OnAwaitingNodeID(Endpoint endpoint)
{
	
	if (!configContext.IsLeader())
		return;

	TryRegisterShardServer(endpoint);
}

void Controller::TryRegisterShardServer(Endpoint& endpoint)
{
	uint64_t		nodeID;
	ConfigCommand	command;

	// first look at existing endpoint => nodeID mapping
	// and at least generate a warning?

	if (configContext.IsAppending())
		return;

	nodeID = nextNodeID++;
	command.RegisterShardServer(nodeID, endpoint);
	configContext.Append(command);		
}
