#include "ConfigHeartbeatManager.h"
#include "System/Events/EventLoop.h"
#include "Application/Common/ContextTransport.h"
#include "ConfigActivationManager.h"
#include "ConfigServer.h"

void ConfigHeartbeatManager::Init(ConfigServer* configServer_)
{
    configServer = configServer_;
    
    heartbeatTimeout.SetCallable(MFUNC(ConfigHeartbeatManager, OnHeartbeatTimeout));
    heartbeatTimeout.SetDelay(1000);
    EventLoop::Add(&heartbeatTimeout);
}

void ConfigHeartbeatManager::Shutdown()
{
    heartbeats.DeleteList();
}

void ConfigHeartbeatManager::OnHeartbeatMessage(ClusterMessage& message)
{
    QuorumPaxosID*      it;
    ConfigQuorum*       quorum;
    ConfigShardServer*  shardServer;
    
    shardServer = configServer->GetDatabaseManager()->GetConfigState()->GetShardServer(message.nodeID);
    if (!shardServer)
        return;
    
    shardServer->quorumPaxosIDs = message.quorumPaxosIDs;
    
    RegisterHeartbeat(message.nodeID);
    
    FOREACH(it, message.quorumPaxosIDs)
    {
        quorum = configServer->GetDatabaseManager()->GetConfigState()->GetQuorum(it->quorumID);
        if (!quorum)
            continue;
            
        if (quorum->paxosID < it->paxosID)
            quorum->paxosID = it->paxosID;
    }

    shardServer->httpPort = message.httpPort;
    shardServer->sdbpPort = message.sdbpPort;
    
    TrySplitShardActions(message);
}

void ConfigHeartbeatManager::OnHeartbeatTimeout()
{
    uint64_t            now;
    Heartbeat*          itHeartbeat;
    ConfigShardServer*  itShardServer;
    
    // OnHeartbeatTimeout() arrives every 1000 msec
    
    now = Now();

    // first remove nodes from the heartbeats list which have
    // not sent a heartbeat in HEARTBEAT_EXPIRE_TIME
    for (itHeartbeat = heartbeats.First(); itHeartbeat != NULL; /* incremented in body */)
    {
        if (itHeartbeat->expireTime <= now)
        {
            CONTEXT_TRANSPORT->DropConnection(itHeartbeat->nodeID);
            Log_Trace("Removing node %U from heartbeats", itHeartbeat->nodeID);
            itHeartbeat = heartbeats.Delete(itHeartbeat);
        }
        else
            break;
    }
    
    if (configServer->GetQuorumProcessor()->IsMaster())
    {
        FOREACH(itShardServer, configServer->GetDatabaseManager()->GetConfigState()->shardServers)
        {
            if (!HasHeartbeat(itShardServer->nodeID))
                configServer->GetActivationManager()->TryDeactivateShardServer(itShardServer->nodeID);
//            else
//                configServer->GetActivationManager()->TryActivateShardServer(itShardServer->nodeID);
        }
    }
    
    EventLoop::Add(&heartbeatTimeout);    
}

bool ConfigHeartbeatManager::HasHeartbeat(uint64_t nodeID)
{
    Heartbeat* it;

    FOREACH(it, heartbeats)
    {
        if (it->nodeID == nodeID)
            return true;
    }

    return false;
}

void ConfigHeartbeatManager::RegisterHeartbeat(uint64_t nodeID)
{
    Heartbeat       *it;
    uint64_t        now;
    
    Log_Trace("Got heartbeat from %U", nodeID);
    
    now = Now();
    
    FOREACH(it, heartbeats)
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
}

void ConfigHeartbeatManager::TrySplitShardActions(ClusterMessage& message)
{
    bool                    isSplitCreating;
    uint64_t                newShardID;
    ConfigState*            configState;
    ConfigShardServer*      configShardServer;
    ConfigQuorum*           itQuorum;
    QuorumShardInfo*        itQuorumShardInfo; 
    
    if (!configServer->GetQuorumProcessor()->IsMaster())
        return;
    
    // look for quorums where the sender of the message is the primary
    // make sure there are no inactive nodes and no splitting going on
    // then look for shards that are located in this quorum and should be split
    // but only one-at-a-time
    
    configState = configServer->GetDatabaseManager()->GetConfigState();
    
    configShardServer = configState->GetShardServer(message.nodeID);
    if (!configShardServer)
        return;
    
    FOREACH(itQuorum, configState->quorums)
    {
        if (itQuorum->primaryID != message.nodeID)
            continue;

        if (itQuorum->isActivatingNode || itQuorum->inactiveNodes.GetLength() > 0)
            continue;
            
        isSplitCreating = IsSplitCreating(itQuorum, newShardID);
        
        FOREACH(itQuorumShardInfo, message.quorumShardInfos)
        {
            if (itQuorumShardInfo->quorumID != itQuorum->quorumID)
                continue;

            if (!isSplitCreating && itQuorumShardInfo->shardSize > SHARD_SPLIT_SIZE)
            {
                configServer->GetQuorumProcessor()->TryShardSplitBegin(
                 itQuorumShardInfo->shardID, itQuorumShardInfo->splitKey);
                return;
            }
            
            if (newShardID == itQuorumShardInfo->shardID)
            {
                configServer->GetQuorumProcessor()->TryShardSplitComplete(
                 itQuorumShardInfo->shardID);
                return;
            }
        }
    }
    
    FOREACH(itQuorumShardInfo, message.quorumShardInfos)
    {
        itQuorum = configState->GetQuorum(itQuorumShardInfo->quorumID);
        if (itQuorum->primaryID == message.nodeID)
        {
            ConfigShard* configShard = configState->GetShard(itQuorumShardInfo->shardID);
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

    FOREACH(itShardID, configQuorum->shards)
    {
        configShard = configState->GetShard(*itShardID);
        assert(configShard != NULL);
        if (configShard->isSplitCreating)
        {
            newShardID = configShard->shardID;
            return true;
        }
    }
    
    return false;
}