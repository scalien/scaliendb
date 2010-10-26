#include "QuorumTransport.h"
#include "Application/Common/ContextTransport.h"

void QuorumTransport::SetQuorum(Quorum* quorum_)
{
    quorum = quorum_;
}

void QuorumTransport::SetQuorumID(uint64_t quorumID_)
{
    quorumID = quorumID_;
}

void QuorumTransport::SendMessage(uint64_t nodeID, Message& msg)
{
    return CONTEXT_TRANSPORT->SendQuorumMessage(nodeID, quorumID, msg);
}

void QuorumTransport::BroadcastMessage(Message& msg)
{
    unsigned        num, i;
    uint64_t        nodeID;
    const uint64_t* nodes;
    
    num = quorum->GetNumNodes();
    nodes = quorum->GetNodes();
    
    for (i = 0; i < num; i++)
    {
        nodeID = nodes[i];
        CONTEXT_TRANSPORT->SendQuorumMessage(nodeID, quorumID, msg);
    }
}
