#include "ShardHeartbeatManager.h"
#include "System/Events/EventLoop.h"
#include "Application/Common/ContextTransport.h"
#include "Application/Common/ClusterMessage.h"
#include "Application/Common/DatabaseConsts.h"
#include "ShardServer.h"
#include "System/Config.h"

#define SHARD_MIGRATION_WRITER (shardServer->GetShardMigrationWriter())

ShardHeartbeatManager::ShardHeartbeatManager()
{
    heartbeatTimeout.SetCallable(MFUNC(ShardHeartbeatManager, OnHeartbeatTimeout));
    heartbeatTimeout.SetDelay(NORMAL_HEARTBEAT_TIMEOUT);    

    activationTimeout.SetCallable(MFUNC(ShardHeartbeatManager, OnActivationTimeout));
    activationTimeout.SetDelay(ACTIVATION_TIMEOUT);
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
    QuorumInfo              quorumInfo;
    List<QuorumInfo>        quorumInfoList;
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
    FOREACH (itQuorumProcessor, *quorumProcessors)
    {
        quorumInfo.quorumID = itQuorumProcessor->GetQuorumID();
        quorumInfo.paxosID = itQuorumProcessor->GetPaxosID();
        quorumInfo.isSendingCatchup = itQuorumProcessor->IsCatchupActive();
        if (quorumInfo.isSendingCatchup)
        {
            quorumInfo.catchupBytesSent = itQuorumProcessor->GetCatchupBytesSent();
            quorumInfo.catchupBytesTotal = itQuorumProcessor->GetCatchupBytesTotal();
            quorumInfo.catchupThroughput = itQuorumProcessor->GetCatchupThroughput();
        }
        else
        {
            quorumInfo.catchupBytesSent = 0;
            quorumInfo.catchupBytesTotal = 0;
            quorumInfo.catchupThroughput = 0;
        }
        quorumInfoList.Append(quorumInfo);
        
        configQuorum = configState->GetQuorum(itQuorumProcessor->GetQuorumID());
        FOREACH (itShardID, configQuorum->shards)
        {
            bool ret = env->ShardExists(QUORUM_DATABASE_DATA_CONTEXT, *itShardID);
            
            if (!ret)
                continue;
            
            quorumShardInfo.quorumID = itQuorumProcessor->GetQuorumID();
            quorumShardInfo.shardID = *itShardID;
            quorumShardInfo.isSplitable = env->IsSplitable(QUORUM_DATABASE_DATA_CONTEXT, *itShardID);
            quorumShardInfo.shardSize = env->GetSize(QUORUM_DATABASE_DATA_CONTEXT, *itShardID);
            quorumShardInfo.splitKey.Write(env->GetMidpoint(QUORUM_DATABASE_DATA_CONTEXT, *itShardID));
            
            if (SHARD_MIGRATION_WRITER->IsActive() && SHARD_MIGRATION_WRITER->GetShardID() == quorumShardInfo.shardID)
            {
                quorumShardInfo.isSendingShard = true;
                quorumShardInfo.migrationQuorumID = SHARD_MIGRATION_WRITER->GetQuorumID();
                quorumShardInfo.migrationNodeID = SHARD_MIGRATION_WRITER->GetNodeID();
                quorumShardInfo.migrationBytesSent = SHARD_MIGRATION_WRITER->GetBytesSent();
                quorumShardInfo.migrationBytesTotal = SHARD_MIGRATION_WRITER->GetBytesTotal();
                quorumShardInfo.migrationThroughput = SHARD_MIGRATION_WRITER->GetThroughput();
            }
            else
            {
                quorumShardInfo.isSendingShard = false;
                quorumShardInfo.migrationQuorumID = 0;
                quorumShardInfo.migrationNodeID = 0;
                quorumShardInfo.migrationBytesSent = 0;
                quorumShardInfo.migrationBytesTotal = 0;
                quorumShardInfo.migrationThroughput = 0;
            }
            quorumShardInfos.Append(quorumShardInfo);
        }
    }

    httpPort = shardServer->GetHTTPPort();
    sdbpPort = shardServer->GetSDBPPort();
    
    msg.Heartbeat(CONTEXT_TRANSPORT->GetSelfNodeID(),
     quorumInfoList, quorumShardInfos, httpPort, sdbpPort);
    shardServer->BroadcastToControllers(msg);

    Log_Trace("Broadcasting heartbeat to controllers");
}

void ShardHeartbeatManager::OnActivation()
{
    if (heartbeatTimeout.GetDelay() == ACTIVATION_HEARTBEAT_TIMEOUT)
        return;
    
    EventLoop::Remove(&heartbeatTimeout);
    OnHeartbeatTimeout();
    
    EventLoop::Remove(&heartbeatTimeout);
    heartbeatTimeout.SetDelay(ACTIVATION_HEARTBEAT_TIMEOUT);
    EventLoop::Add(&heartbeatTimeout);
    
    EventLoop::Reset(&activationTimeout);
}

void ShardHeartbeatManager::OnActivationTimeout()
{
    EventLoop::Remove(&heartbeatTimeout);
    heartbeatTimeout.SetDelay(NORMAL_HEARTBEAT_TIMEOUT);
    EventLoop::Add(&heartbeatTimeout);
}
