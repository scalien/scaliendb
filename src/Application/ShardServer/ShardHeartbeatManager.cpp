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
    List<QuorumPaxosID>     quorumPaxosIDList;
    List<QuorumShardInfo>   quorumShardInfos;
    QuorumShardInfo         quorumShardInfo;
    ConfigState*            configState;
    ConfigQuorum*           configQuorum;
    StorageEnvironment*     env;
    
    Log_Trace();
    
    EventLoop::Add(&heartbeatTimeout);
    
    if (CONTEXT_TRANSPORT->IsAwaitingNodeID())
    {
        Log_Trace("not sending heartbeat");
        return;
    }
    
    configState = shardServer->GetConfigState();
    env = shardServer->GetDatabaseManager()->GetEnvironment();

    ShardServer::QuorumProcessorList* quorumProcessors = shardServer->GetQuorumProcessors();
    FOREACH(itQuorumProcessor, *quorumProcessors)
    {
        quorumPaxosID.quorumID = itQuorumProcessor->GetQuorumID();
        quorumPaxosID.paxosID = itQuorumProcessor->GetPaxosID();
        quorumPaxosIDList.Append(quorumPaxosID);
        
        configQuorum = configState->GetQuorum(itQuorumProcessor->GetQuorumID());
        FOREACH(itShardID, configQuorum->shards)
        {
            bool ret = env->ShardExists(QUORUM_DATABASE_DATA_CONTEXT, *itShardID);
            
            if (!ret)
                continue;
            
            quorumShardInfo.quorumID = itQuorumProcessor->GetQuorumID();
            quorumShardInfo.shardID = *itShardID;
            quorumShardInfo.isSplitable = env->IsSplitable(QUORUM_DATABASE_DATA_CONTEXT, *itShardID);
            quorumShardInfo.shardSize = env->GetSize(QUORUM_DATABASE_DATA_CONTEXT, *itShardID);
            quorumShardInfo.splitKey.Write(env->GetMidpoint(QUORUM_DATABASE_DATA_CONTEXT, *itShardID));
            
            quorumShardInfos.Append(quorumShardInfo);
        }
    }

    httpPort = shardServer->GetHTTPPort();
    sdbpPort = shardServer->GetSDBPPort();
    
    msg.Heartbeat(CONTEXT_TRANSPORT->GetSelfNodeID(),
     quorumPaxosIDList, quorumShardInfos, httpPort, sdbpPort);
    shardServer->BroadcastToControllers(msg);
}
