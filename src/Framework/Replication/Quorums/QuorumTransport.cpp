#include "QuorumTransport.h"

QuorumTransport::QuorumTransport()
{
	priority = false;
}

void QuorumTransport::SetPriority(bool priority_)
{
	priority = priority_;
}

void QuorumTransport::SetPrefix(const Buffer& prefix_)
{
	prefix.Write(prefix_);
}

void QuorumTransport::SetReplicationTransport(ReplicationTransport* transport_)
{
	transport = transport_;
}

void QuorumTransport::SetQuorum(Quorum* quorum_)
{
	quorum = quorum_;
}

ReadBuffer QuorumTransport::GetMessage() const
{
	return transport->GetMessage();
}

void QuorumTransport::SendMessage(uint64_t nodeID, const Message& msg)
{
	if (priority)
		return transport->SendPriorityMessage(nodeID, prefix, msg);
	else
		return transport->SendMessage(nodeID, prefix, msg);
}

void QuorumTransport::BroadcastMessage(const Message& msg)
{
	unsigned		num, i, nodeID;
	const uint64_t*	nodes;
	
	num = quorum->GetNumNodes();
	nodes = quorum->GetNodes();
	
	for (i = 0; i < num; i++)
	{
		nodeID = nodes[i];
		if (priority)
			return transport->SendPriorityMessage(nodeID, prefix, msg);
		else
			return transport->SendMessage(nodeID, prefix, msg);
	}
}
