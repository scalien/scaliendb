#include "ShardHeartbeatManager.h"
#include "System/Events/EventLoop.h"
#include "Application/Common/ContextTransport.h"
#include "Application/Common/ClusterMessage.h"
#include "ShardServer.h"

ShardHeartbeatManager::ShardHeartbeatManager()
{
    heartbeatTimeout.SetCallable(MFUNC(ShardHeartbeatManager, OnHeartbeatTimeout));
    heartbeatTimeout.SetDelay(HEARTBEAT_TIMEOUT);    
}

void ShardHeartbeatManager::Init(ShardServer* shardServer_)
{
    EventLoop::Add(&heartbeatTimeout);
    
    shardServer = shardServer_;
}

void ShardHeartbeatManager::OnHeartbeatTimeout()
{
    ShardQuorumProcessor*   itQuorumProcessor;
    ClusterMessage          msg;
    QuorumPaxosID           quorumPaxosID;
    QuorumPaxosID::List     quorumPaxosIDList;
    
    Log_Trace();
    
    EventLoop::Add(&heartbeatTimeout);
    
    if (CONTEXT_TRANSPORT->IsAwaitingNodeID())
    {
        Log_Trace("not sending heartbeat");
        return;
    }

    ShardServer::QuorumProcessorList* quorumProcessors = shardServer->GetQuorumProcessors();
    FOREACH(itQuorumProcessor, *quorumProcessors)
    {
        quorumPaxosID.quorumID = itQuorumProcessor->GetQuorumID();
        quorumPaxosID.paxosID = itQuorumProcessor->GetPaxosID();
        quorumPaxosIDList.Append(quorumPaxosID);
    }
    
    msg.Heartbeat(CONTEXT_TRANSPORT->GetSelfNodeID(), quorumPaxosIDList);
    shardServer->BroadcastToControllers(msg);
}
