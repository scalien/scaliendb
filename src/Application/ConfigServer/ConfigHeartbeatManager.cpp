#include "ConfigHeartbeatManager.h"
#include "System/Config.h"
#include "System/Events/EventLoop.h"
#include "Application/Common/ContextTransport.h"
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
}

void ConfigHeartbeatManager::Shutdown()
{
    heartbeats.DeleteList();
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
    
    // OnHeartbeatTimeout() arrives every 1000 msec
    
    now = Now();

    // first remove nodes from the heartbeats list which have
    // not sent a heartbeat in HEARTBEAT_EXPIRE_TIME
    num = heartbeats.GetLength();
    for (itHeartbeat = heartbeats.First(); itHeartbeat != NULL; /* incremented in body */)
    {
        if (itHeartbeat->expireTime <= now)
        {
            CONTEXT_TRANSPORT->DropConnection(itHeartbeat->nodeID);
            Log_Debug("Removing node %U from heartbeats list", itHeartbeat->nodeID);
            itShardServer = CONFIG_STATE->GetShardServer(itHeartbeat->nodeID);
            // if the shard server was unregistered, itShardServer is NULL here
            if (itShardServer != NULL)
                itShardServer->hasHeartbeat = false;
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
    Heartbeat       *it;
    uint64_t        now;
    
    Log_Trace("Got heartbeat from %U", nodeID);
    
    now = Now();
    
    FOREACH (it, heartbeats)
    {
        if (it->nodeID == nodeID)
        {
            heartbeats.Remove(it);
            it->expireTime = now + HEARTBEAT_EXPIRE_TIME;
            heartbeats.Add(it);
            return;
        }
    }
    
    it = new Heartbeat();
    it->nodeID = nodeID;
    it->expireTime = now + HEARTBEAT_EXPIRE_TIME;
    heartbeats.Add(it);
    
    configServer->OnConfigStateChanged();
}

void ConfigHeartbeatManager::TrySplitShardActions(ClusterMessage& message)
{
    bool                    isSplitCreating;
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
    
    FOREACH (itQuorum, CONFIG_STATE->quorums)
    {
        if (itQuorum->primaryID != message.nodeID)
            continue;

        if (itQuorum->isActivatingNode || itQuorum->inactiveNodes.GetLength() > 0)
            continue;
            
        // TODO: tidy up
        newShardID = 0;
        isSplitCreating = IsSplitCreating(itQuorum, newShardID);
        isSplitCreating |= configServer->GetDatabaseManager()->GetConfigState()->isSplitting;
        
        FOREACH (itQuorumShardInfo, message.quorumShardInfos)
        {
            if (itQuorumShardInfo->quorumID != itQuorum->quorumID)
                continue;
                
            configTable = CONFIG_STATE->GetTable(itQuorumShardInfo->shardID);
            if (configTable != NULL)
            {
                if (configTable->shards.GetLength() == 1)
                {
                    configShard = CONFIG_STATE->GetShard(*(configTable->shards.First()));
                    ASSERT(configShard);
                    if (configShard->state == CONFIG_SHARD_STATE_TRUNC_CREATING)
                        configServer->GetQuorumProcessor()->TryTruncateTableComplete(configTable->tableID);
                }
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
                    configServer->GetQuorumProcessor()->TrySplitShardBegin(
                     itQuorumShardInfo->shardID, ReadBuffer(itQuorumShardInfo->splitKey));
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
