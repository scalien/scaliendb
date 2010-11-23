#include "ConfigSplitShardManager.h"
#include "ConfigServer.h"

void ConfigSplitShardManager::Init(ConfigServer* configServer_)
{
    configServer = configServer_;
}

void ConfigSplitShardManager::OnHeartbeatMessage(ClusterMessage& message)
{
    ConfigState*            configState;
    ConfigShardServer*      configShardServer;
    ConfigQuorum*           itQuorum;
    QuorumShardInfo*        itQuorumShardInfo;
    
    if (!configServer->GetQuorumProcessor()->IsMaster())
        return;
    
    // look for quorums where the sender of the message is the primary
    // make sure there are no inactive nodes and no splitting going on
    // then look for shards that are located in this quorum and should be split
    
    configState = configServer->GetDatabaseManager()->GetConfigState();
    
    configShardServer = configState->GetShardServer(message.nodeID);
    if (!configShardServer)
        return;
    
    FOREACH(itQuorum, configState->quorums)
    {
        if (itQuorum->primaryID != message.nodeID)
            continue;

        if (itQuorum->isActivatingNode || itQuorum->inactiveNodes.GetLength())
            continue;
            
        if (IsSplitCreating(itQuorum))
            continue;
        
        FOREACH(itQuorumShardInfo, message.quorumShardInfos)
        {
            if (itQuorumShardInfo->quorumID != itQuorum->quorumID)
                continue;

            if (itQuorumShardInfo->shardSize > SHARD_SPLIT_SIZE)
            {
                configServer->GetQuorumProcessor()->TryShardSplit(
                 itQuorumShardInfo->shardID, itQuorumShardInfo->splitKey);
                return;
            }
        }
    }
}

bool ConfigSplitShardManager::IsSplitCreating(ConfigQuorum* configQuorum)
{
    uint64_t*           itShardID;
    ConfigState*        configState;
    ConfigShard*        configShard;

    configState = configServer->GetDatabaseManager()->GetConfigState();

    FOREACH(itShardID, configQuorum->shards)
    {
        configShard = configState->GetShard(*itShardID);
        if (configShard->isSplitCreating)
            return true;
    }
    
    return false;
}