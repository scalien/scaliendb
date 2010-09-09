#include "QuorumTransport.h"
#include "Framework/Replication/ContextTransport.h"

QuorumTransport::QuorumTransport()
{
	priority = false;
}

void QuorumTransport::SetPriority(bool priority_)
{
	priority = priority_;
}

void QuorumTransport::SetPrefix(Buffer& prefix_)
{
	prefix.Write(prefix_);
}

void QuorumTransport::SetQuorum(Quorum* quorum_)
{
	quorum = quorum_;
}

void QuorumTransport::SendMessage(uint64_t nodeID, Message& msg)
{
	if (priority)
		return CONTEXT_TRANSPORT->SendPriorityMessage(nodeID, prefix, msg);
	else
		return CONTEXT_TRANSPORT->SendMessage(nodeID, prefix, msg);
}

void QuorumTransport::BroadcastMessage(Message& msg)
{
	unsigned		num, i;
	uint64_t		nodeID;
	const uint64_t*	nodes;
	
	num = quorum->GetNumNodes();
	nodes = quorum->GetNodes();
	
	for (i = 0; i < num; i++)
	{
		nodeID = nodes[i];
		if (priority)
			CONTEXT_TRANSPORT->SendPriorityMessage(nodeID, prefix, msg);
		else
			CONTEXT_TRANSPORT->SendMessage(nodeID, prefix, msg);
	}
}
