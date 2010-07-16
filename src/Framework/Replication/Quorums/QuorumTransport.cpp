#include "QuorumTransport.h"

QuorumTransport::QuorumTransport()
{
	priority = false;
}

void QuorumTransport::SetPriority(bool priority_)
{
	priority = priority_;
}

void QuorumTransport::SetReplicationTransport(ReplicationTransport* transport_)
{
	transport = transport_;
}

void QuorumTransport::SetQuorum(Quorum* quorum_)
{
	quorum = quorum_;
}

ReplicationMessage*	QuorumTransport::GetMessage() const
{
	return transport->GetMessage();
}

void QuorumTransport::SendMessage(unsigned nodeID, const ReplicationMessage& msg)
{
	if (priority)
		return transport->SendPriorityMessage(nodeID, msg);
	else
		return transport->SendMessage(nodeID, msg);
}

void QuorumTransport::BroadcastMessage(const ReplicationMessage& msg)
{
	unsigned		num, i, nodeID;
	const unsigned*	nodes;
	
	num = quorum->GetNumNodes();
	nodes = quorum->GetNodes();
	
	for (i = 0; i < num; i++)
	{
		nodeID = nodes[i];
		if (priority)
			return transport->SendPriorityMessage(nodeID, msg);
		else
			return transport->SendMessage(nodeID, msg);
	}
}
