#include "Controller.h"
#include "ConfigMessage.h"
#include "System/Config.h"
#include "Framework/Replication/ReplicationConfig.h"
#include "Framework/Replication/PaxosLease/PaxosLease.h"
#include "Application/Common/ContextTransport.h"
#include "Application/Common/ClientSession.h"
#include "Application/Common/ClusterMessage.h"

void Controller::Init()
{
	unsigned		numControllers;
	int64_t			nodeID;
	const char*		str;
	Endpoint		endpoint;
	
	primaryLeaseTimeout.SetCallable(MFUNC(Controller, OnPrimaryLeaseTimeout));
	
	nodeID = configFile.GetIntValue("nodeID", -1);
	if (nodeID < 0)
		ASSERT_FAIL();
	
	CONTEXT_TRANSPORT->SetSelfNodeID(nodeID);
	REPLICATION_CONFIG->SetNodeID(nodeID);
	
	CONTEXT_TRANSPORT->SetClusterContext(this);
	
	InitConfigContext();
	
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

bool Controller::ProcessClientCommand(ClientSession* /*conn*/, ConfigMessage& message)
{
	ConfigMessage*	pmessage;

	if (!configState.CompleteMessage(message))
	{
		// TODO: send failed to conn
	}
	
	pmessage = new ConfigMessage;
	*pmessage = message;
	configMessages.Append(pmessage);
	
	if (!configContext.IsAppending())
		configContext.Append(configMessages.First());
	
	return true;
}

void Controller::OnLearnLease()
{
	Endpoint endpoint;
	
	if (CONTEXT_TRANSPORT->GetNextWaiting(endpoint))
		TryRegisterShardServer(endpoint);
}

void Controller::OnConfigMessage(ConfigMessage& message)
{
	bool			status;
	ClusterMessage	clusterMessage;
	
	if (message.type == CONFIG_REGISTER_SHARDSERVER)
	{
		// tell ContextTransport that this connection has a new nodeID
		CONTEXT_TRANSPORT->SetConnectionNodeID(message.endpoint, message.nodeID);
		
		// tell the shard server
		clusterMessage.SetNodeID(message.nodeID);
		CONTEXT_TRANSPORT->SendClusterMessage(message.nodeID, clusterMessage);
	}
	
	status = configState.OnMessage(message);
	
	SendClientReply(message);
}

void Controller::OnClusterMessage(uint64_t /*nodeID*/, ClusterMessage& message)
{
	if (!IsMaster())
		return;	

	switch (message.type)
	{
		case CLUSTERMESSAGE_SET_NODEID:
			ASSERT_FAIL();
		case CLUSTERMESSAGE_SET_CONFIG_STATE:
			ASSERT_FAIL();
		case CLUSTERMESSAGE_REQUEST_LEASE:
			OnRequestLease(message);
			break;
		case CLUSTERMESSAGE_RECEIVE_LEASE:
			ASSERT_FAIL();
	}
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

void Controller::OnPrimaryLeaseTimeout()
{
	uint64_t		now;
	PrimaryLease*	it;
	ConfigQuorum*	quorum;
	
	now = EventLoop::Now();
	
	for (it = primaryLeases.First(); it != NULL;)
	{
		if (it->expireTime < now)
		{
			quorum = configState.GetQuorum(it->quorumID);
			quorum->hasPrimary = false;
			quorum->primaryID = 0;
			it = primaryLeases.Delete(it);
		}
	}
	
	UpdatePrimaryLeaseTimer();
	
	// TODO: should the controller inform someone about this?
}

void Controller::InitConfigContext()
{
}

void Controller::TryRegisterShardServer(Endpoint& endpoint)
{
	ConfigMessage*	message;

	if (configContext.IsAppending())
		return;

	message->RegisterShardServer(0, endpoint);
	if (!configState.CompleteMessage(*message))
	{
		// TODO: ?
	}

	configMessages.Append(message);
	
	if (!configContext.IsAppending())
		configContext.Append(configMessages.First());
}

void Controller::SendClientReply(ConfigMessage& message)
{
}

void Controller::OnRequestLease(ClusterMessage& message)
{
	uint64_t*		it;
	ConfigQuorum*	quorum;
	
	quorum = configState.GetQuorum(message.quorumID);
	
	if (quorum == NULL)
	{
		Log_Trace("nodeID " PRIu64 " requesting lease for non-existing quorum %" PRIu64 "",
		 message.nodeID, message.quorumID);
		return;
	}
	
	if (quorum->hasPrimary)
	{
		if (quorum->primaryID != message.nodeID)
			return;
		else
			ExtendPrimaryLease(*quorum, message);
	}
	
	for (it = quorum->activeNodes.First(); it != NULL; it = quorum->activeNodes.Next(it))
	{
		if (*it == message.nodeID)
			break;
	}
	
	if (it == NULL)
	{
		Log_Trace("nodeID " PRIu64 " requesting lease but not active member or quorum %" PRIu64 "",
		 message.nodeID, message.quorumID);
		return;
	}
	
	AssignPrimaryLease(*quorum, message);
}

void Controller::AssignPrimaryLease(ConfigQuorum& quorum, ClusterMessage& message)
{
	unsigned		duration;
	PrimaryLease*	primaryLease;
	ClusterMessage	response;

	quorum.hasPrimary = true;
	quorum.primaryID = message.nodeID;

	primaryLease = new PrimaryLease;
	primaryLease->quorumID = quorum.quorumID;
	primaryLease->nodeID = quorum.primaryID;
	duration = MIN(message.duration, PAXOSLEASE_MAX_LEASE_TIME);
	primaryLease->expireTime = EventLoop::Now() + duration;
	primaryLeases.Add(primaryLease);

	UpdatePrimaryLeaseTimer();

	response.ReceiveLease(message.nodeID, message.quorumID, message.proposalID, duration);
	CONTEXT_TRANSPORT->SendClusterMessage(message.nodeID, message);
}

void Controller::ExtendPrimaryLease(ConfigQuorum& quorum, ClusterMessage& message)
{
	unsigned		duration;
	PrimaryLease*	it;
	ClusterMessage	response;

	duration = MIN(message.duration, PAXOSLEASE_MAX_LEASE_TIME);

	for (it = primaryLeases.First(); it != NULL; it = primaryLeases.Next(it))
	{
		if (it->quorumID == quorum.quorumID)
			break;
	}
	
	assert(it != NULL);
	
	primaryLeases.Remove(it);
	duration = MIN(message.duration, PAXOSLEASE_MAX_LEASE_TIME);
	it->expireTime = EventLoop::Now() + duration;
	primaryLeases.Add(it);
	UpdatePrimaryLeaseTimer();

	response.ReceiveLease(message.nodeID, message.quorumID, message.proposalID, duration);
	CONTEXT_TRANSPORT->SendClusterMessage(message.nodeID, message);
}

void Controller::UpdatePrimaryLeaseTimer()
{
	PrimaryLease* primaryLease;

	primaryLease = primaryLeases.First();
	
	if (primaryLease->expireTime < primaryLeaseTimeout.GetExpireTime())
	{
		EventLoop::Remove(&primaryLeaseTimeout);
		primaryLeaseTimeout.SetExpireTime(primaryLease->expireTime);
		EventLoop::Add(&primaryLeaseTimeout);
	}
}
