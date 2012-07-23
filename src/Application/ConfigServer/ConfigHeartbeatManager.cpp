#include "ConfigHeartbeatManager.h"
#include "System/Config.h"
#include "System/Events/EventLoop.h"
#include "Application/Common/ContextTransport.h"
#include "Application/Common/DatabaseConsts.h"
#include "ConfigActivationManager.h"
#include "ConfigServer.h"

#define CONFIG_STATE (configServer->GetDatabaseManager()->GetConfigState())

void ConfigHeartbeatManager::Init(ConfigServer* configServer_)
{
    configServer = configServer_;
    
    heartbeatTimeout.SetCallable(MFUNC(ConfigHeartbeatManager, OnHeartbeatTimeout));
    heartbeatTimeout.SetDelay(1000);
    EventLoop::Add(&heartbeatTimeout);
    
    shardSplitSize = configFile.GetIntValue("shardSplitSize", DEFAULT_SHARD_SPLIT_SIZE);
    heartbeatExpireTimeout = configFile.GetIntValue("heartbeatExpireTimeout", HEARTBEAT_EXPIRE_TIME);
    shardSplitCooldownTime = configFile.GetIntValue("shardSplitCooldownTime", SPLIT_COOLDOWN_TIME);

    lastSplitTime = 0;
}

void ConfigHeartbeatManager::Shutdown()
{
    heartbeats.DeleteList();
}

void ConfigHeartbeatManager::SetShardSplitSize(uint64_t shardSplitSize_)
{
    shardSplitSize = shardSplitSize_;
}

uint64_t ConfigHeartbeatManager::GetShardSplitSize()
{
    return shardSplitSize;
}

void ConfigHeartbeatManager::SetHeartbeatExpireTimeout(uint64_t heartbeatExpireTimeout_)
{
    heartbeatExpireTimeout = heartbeatExpireTimeout_;
}

uint64_t ConfigHeartbeatManager::GetHeartbeatExpireTimeout()
{
    return heartbeatExpireTimeout;
}

void ConfigHeartbeatManager::SetShardSplitCooldownTime(uint64_t shardSplitCooldownTime_)
{
    shardSplitCooldownTime = shardSplitCooldownTime_;
}

uint64_t ConfigHeartbeatManager::GetShardSplitCooldownTime()
{
    return shardSplitCooldownTime;
}

void ConfigHeartbeatManager::OnHeartbeatMessage(ClusterMessage& message)
{
    QuorumInfo*         it;
    ConfigQuorum*       quorum;
    ConfigShardServer*  shardServer;
    
    shardServer = CONFIG_STATE->GetShardServer(message.nodeID);
    if (!shardServer)
        return;
    
    shardServer->quorumInfos.Clear();
    shardServer->quorumInfos = message.quorumInfos;

    shardServer->quorumShardInfos.Clear();
    shardServer->quorumShardInfos = message.quorumShardInfos;
        
    RegisterHeartbeat(message.nodeID);
    
    FOREACH (it, message.quorumInfos)
    {
        quorum = CONFIG_STATE->GetQuorum(it->quorumID);
        if (!quorum)
            continue;
            
        if (quorum->paxosID < it->paxosID)
            quorum->paxosID = it->paxosID;
    }
    
    shardServer->httpPort = message.httpPort;
    shardServer->sdbpPort = message.sdbpPort;
    shardServer->hasHeartbeat = true;
    
    configServer->OnConfigStateChanged();
    
    TrySplitShardActions(message);
    
    configServer->GetActivationManager()->TryActivateShardServer(message.nodeID, false);

    Log_Trace("Received heartbeat from node %U", message.nodeID);
}

void ConfigHeartbeatManager::OnHeartbeatTimeout()
{
    uint64_t            now, num;
    Heartbeat*          itHeartbeat;
    ConfigShardServer*  itShardServer;
    ConfigQuorum*       configQuorum;
    
    // OnHeartbeatTimeout() arrives every 1000 msec
    
    now = Now();

    // first remove nodes from the heartbeats list which have
    // not sent a heartbeat in heartbeatExpireTimeout
    num = heartbeats.GetLength();
    for (itHeartbeat = heartbeats.First(); itHeartbeat != NULL; /* incremented in body */)
    {
        if (itHeartbeat->expireTime <= now)
        {
            CONTEXT_TRANSPORT->DropConnection(itHeartbeat->nodeID);
            Log_Message("Removing node %U from heartbeats list", itHeartbeat->nodeID);
            itShardServer = CONFIG_STATE->GetShardServer(itHeartbeat->nodeID);
            // if the shard server was unregistered, itShardServer is NULL here
            if (itShardServer != NULL)
                itShardServer->hasHeartbeat = false;

            // remove this node's primary lease
            // this code should really be in ConfigPrimaryLeaseManager
            FOREACH(configQuorum, CONFIG_STATE->quorums)
            {
                if (configQuorum->hasPrimary && configQuorum->primaryID == itHeartbeat->nodeID)
                {
                    configQuorum->hasPrimary = false;
                    configQuorum->primaryID = 0;
                }
            }

            itHeartbeat = heartbeats.Delete(itHeartbeat);
        }
        else
            break;
    }
    
    if (configServer->GetQuorumProcessor()->IsMaster())
    {
        FOREACH (itShardServer, CONFIG_STATE->shardServers)
        {
            if (!HasHeartbeat(itShardServer->nodeID))
                configServer->GetActivationManager()->TryDeactivateShardServer(itShardServer->nodeID);
        }
    }
    
    if (heartbeats.GetLength() != (signed) num)
        configServer->OnConfigStateChanged();
    
    EventLoop::Add(&heartbeatTimeout);    
}

bool ConfigHeartbeatManager::HasHeartbeat(uint64_t nodeID)
{
    Heartbeat* it;

    FOREACH (it, heartbeats)
    {
        if (it->nodeID == nodeID)
            return true;
    }

    return false;
}

unsigned ConfigHeartbeatManager::GetNumHeartbeats()
{
    return heartbeats.GetLength();
}

ConfigHeartbeatManager::HeartbeatList& ConfigHeartbeatManager::GetHeartbeats()
{
    return heartbeats;
}

void ConfigHeartbeatManager::RegisterHeartbeat(uint64_t nodeID)
{
    uint64_t    now;
    Heartbeat*  heartbeat;
    
    Log_Trace("Got heartbeat from %U", nodeID);
    
    now = Now();
    
    FOREACH (heartbeat, heartbeats)
    {
        if (heartbeat->nodeID == nodeID)
        {
            heartbeats.Remove(heartbeat);
            heartbeat->expireTime = now + heartbeatExpireTimeout;
            heartbeats.Add(heartbeat);
            return;
        }
    }
    
    heartbeat = new Heartbeat;
    heartbeat->nodeID = nodeID;
    heartbeat->expireTime = now + heartbeatExpireTimeout;
    heartbeats.Add(heartbeat);
    
    configServer->OnConfigStateChanged();
}

void ConfigHeartbeatManager::TrySplitShardActions(ClusterMessage& message)
{
    bool                    isSplitCreating;
    uint64_t                now;
    uint64_t                newShardID;
    ConfigShardServer*      configShardServer;
    ConfigQuorum*           itQuorum;
    ConfigShard*            configShard;
    ConfigTable*            configTable;
    QuorumShardInfo*        itQuorumShardInfo; 
    
    if (!configServer->GetQuorumProcessor()->IsMaster())
        return;
    
    // look for quorums where the sender of the message is the primary
    // make sure there are no inactive nodes and no splitting going on
    // then look for shards that are located in this quorum and should be split
    // but only one-at-a-time
    
    configShardServer = CONFIG_STATE->GetShardServer(message.nodeID);
    if (!configShardServer)
        return;
    
    now = EventLoop::Now();

    FOREACH (itQuorum, CONFIG_STATE->quorums)
    {
        if (itQuorum->primaryID != message.nodeID)
            continue;
            
        // TODO: tidy up
        newShardID = 0;
        isSplitCreating = IsSplitCreating(itQuorum, newShardID);
        isSplitCreating |= configServer->GetDatabaseManager()->GetConfigState()->isSplitting;
        
        FOREACH (itQuorumShardInfo, message.quorumShardInfos)
        {
            if (itQuorumShardInfo->quorumID != itQuorum->quorumID)
                continue;
            
            configShard = CONFIG_STATE->GetShard(itQuorumShardInfo->shardID);
            if (configShard)
            {
                if (configShard->state == CONFIG_SHARD_STATE_TRUNC_CREATING)
                    configServer->GetQuorumProcessor()->TryTruncateTableComplete(configShard->tableID);
            }
                
            if (!isSplitCreating && itQuorumShardInfo->isSplitable &&
             itQuorumShardInfo->shardSize > shardSplitSize &&
             itQuorumShardInfo->shardID != CONFIG_STATE->migrateSrcShardID)
            {
                // make sure another shard with the same splitKey doesn't already exist
                configShard = CONFIG_STATE->GetShard(itQuorumShardInfo->shardID);
                if (!configShard)
                    continue;
                configTable = CONFIG_STATE->GetTable(configShard->tableID);
                ASSERT(configTable != NULL);
                if (configTable->isFrozen)
                    continue;
                if (!configServer->GetDatabaseManager()->ShardExists(
                 configShard->tableID, ReadBuffer(itQuorumShardInfo->splitKey)))
                {
                    if (now > (lastSplitTime + shardSplitCooldownTime))
                    {
                        configServer->GetQuorumProcessor()->TrySplitShardBegin(
                            itQuorumShardInfo->shardID, ReadBuffer(itQuorumShardInfo->splitKey));
                        lastSplitTime = now;
                    }
                    break;
                }
            }
            
            if (newShardID == itQuorumShardInfo->shardID)
            {
                configServer->GetQuorumProcessor()->TrySplitShardComplete(
                 itQuorumShardInfo->shardID);
                break;
            }
        }
    }
    
    FOREACH (itQuorumShardInfo, message.quorumShardInfos)
    {
        itQuorum = CONFIG_STATE->GetQuorum(itQuorumShardInfo->quorumID);
        if (itQuorum->primaryID == message.nodeID)
        {
            ConfigShard* configShard = CONFIG_STATE->GetShard(itQuorumShardInfo->shardID);
            if (!configShard)
                continue;
            configShard->isSplitable = itQuorumShardInfo->isSplitable;
            configShard->shardSize = itQuorumShardInfo->shardSize;
            configShard->splitKey.Write(itQuorumShardInfo->splitKey);
        }
    }
}

bool ConfigHeartbeatManager::IsSplitCreating(ConfigQuorum* configQuorum, uint64_t& newShardID)
{
    uint64_t*           itShardID;
    ConfigState*        configState;
    ConfigShard*        configShard;

    configState = configServer->GetDatabaseManager()->GetConfigState();

    FOREACH (itShardID, configQuorum->shards)
    {
        configShard = configState->GetShard(*itShardID);
        ASSERT(configShard != NULL);
        if (configShard->state == CONFIG_SHARD_STATE_SPLIT_CREATING)
        {
            newShardID = configShard->shardID;
            return true;
        }
    }
    
    return false;
}
