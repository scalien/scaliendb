#include "Controller.h"
#include "ConfigCommand.h"
#include "Application/Common/ContextTransport.h"
#include "Application/Common/ClientConnection.h"
#include "Application/Common/ClusterMessage.h"

void Controller::Init()
{
	//nextNodeID = table.GetUint64("nextNodeID");
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

bool Controller::ProcessClientCommand(ClientConnection* conn, ConfigCommand& command)
{
	return true;
}

void Controller::OnConfigCommand(ConfigCommand& command)
{
	ClusterMessage clusterMessage;
	
	if (command.type == CONFIG_REGISTER_SHARDSERVER)
	{
		// tell ContextTransport that this connection has a new nodeID
		CONTEXT_TRANSPORT->SetNodeID(command.endpoint, command.nodeID);
		
		// tell the shard server
		clusterMessage.SetNodeID(command.nodeID);
		CONTEXT_TRANSPORT->SendClusterMessage(command.nodeID, clusterMessage);
	}
}

void Controller::OnClusterMessage(uint64_t nodeID, ClusterMessage& msg)
{
	clusterState.Update(msg);
}

void Controller::OnIncomingConnectionReady(uint64_t /*nodeID*/, Endpoint /*endpoint*/)
{
}

void Controller::OnAwaitingNodeID(Endpoint endpoint)
{	
	uint64_t		nodeID;
	ConfigCommand	command;
	
	if (!configContext.IsLeader())
	{
		return;
	}

	nodeID = nextNodeID++;
	command.RegisterShardServer(nodeID, endpoint);
	configContext.Append(command);		
}
