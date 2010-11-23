#include "ShardHeartbeatManager.h"
#include "System/Events/EventLoop.h"
#include "Application/Common/ContextTransport.h"
#include "Application/Common/ClusterMessage.h"
#include "ShardServer.h"
#include "System/Config.h"

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
    unsigned                httpPort;
    unsigned                sdbpPort;
    uint64_t*               itShardID;
    ShardQuorumProcessor*   itQuorumProcessor;
    ClusterMessage          msg;
    QuorumPaxosID           quorumPaxosID;
    QuorumPaxosID::List     quorumPaxosIDList;
    QuorumShardInfo         quorumShardInfo;
    QuorumShardInfo::List   quorumShardInfos;
    StorageShard*           storageShard;
    ConfigState*            configState;
    ConfigQuorum*           configQuorum;
    
    Log_Trace();
    
    EventLoop::Add(&heartbeatTimeout);
    
    if (CONTEXT_TRANSPORT->IsAwaitingNodeID())
    {
        Log_Trace("not sending heartbeat");
        return;
    }
    
    configState = shardServer->GetConfigState();

    ShardServer::QuorumProcessorList* quorumProcessors = shardServer->GetQuorumProcessors();
    FOREACH(itQuorumProcessor, *quorumProcessors)
    {
        quorumPaxosID.quorumID = itQuorumProcessor->GetQuorumID();
        quorumPaxosID.paxosID = itQuorumProcessor->GetPaxosID();
        quorumPaxosIDList.Append(quorumPaxosID);
        
        configQuorum = configState->GetQuorum(itQuorumProcessor->GetQuorumID());
        FOREACH(itShardID, configQuorum->shards)
        {
            storageShard = shardServer->GetDatabaseManager()->GetShard(*itShardID);
            if (!storageShard)
                continue;
            
            quorumShardInfo.quorumID = quorumPaxosID.quorumID;
            quorumShardInfo.shardID = *itShardID;
            quorumShardInfo.shardSize = storageShard->GetSize();
            storageShard->GetMidpoint(quorumShardInfo.splitKey);
            
            quorumShardInfos.Append(quorumShardInfo);
        }
    }

    httpPort = shardServer->GetHTTPPort();
    sdbpPort = shardServer->GetSDBPPort();
    
    msg.Heartbeat(CONTEXT_TRANSPORT->GetSelfNodeID(), quorumPaxosIDList, quorumShardInfos, httpPort, sdbpPort);
    shardServer->BroadcastToControllers(msg);
}
