#include "ConfigActivationManager.h"
#include "System/Events/EventLoop.h"
#include "Application/Common/DatabaseConsts.h"
#include "Application/Common/ContextTransport.h"
#include "ConfigServer.h"
#include "ConfigQuorumProcessor.h"

#define CONFIG_STATE        (configServer->GetDatabaseManager()->GetConfigState())
#define QUORUM_PROCESSOR    (configServer->GetQuorumProcessor())
#define HEARTBEAT_MANAGER   (configServer->GetHeartbeatManager())

void ConfigActivationManager::Init(ConfigServer* configServer_)
{
    configServer = configServer_;
    
    activationTimeout.SetCallable(MFUNC(ConfigActivationManager, OnActivationTimeout));
}

void ConfigActivationManager::TryDeactivateShardServer(uint64_t nodeID, bool force)
{
    ConfigQuorum*       quorum;
    ConfigShardServer*  shardServer;

    FOREACH (quorum, CONFIG_STATE->quorums)
    {
        if (quorum->isActivatingNode && quorum->activatingNodeID == nodeID)
        {
            shardServer = CONFIG_STATE->GetShardServer(quorum->activatingNodeID);
            ASSERT(shardServer != NULL);

            quorum->ClearActivation();
            UpdateTimeout();

            Log_Message("Activation failed for shard server %U and quorum %U...",
             quorum->quorumID, quorum->activatingNodeID);
        }

        if (quorum->IsActiveMember(nodeID))
        {
            // if this node was part of an activation process, cancel it
            if (quorum->isActivatingNode)
            {
                shardServer = CONFIG_STATE->GetShardServer(quorum->activatingNodeID);
                ASSERT(shardServer != NULL);

                quorum->ClearActivation();
                UpdateTimeout();

                Log_Message("Activation failed for shard server %U and quorum %U...",
                 quorum->activatingNodeID, quorum->quorumID);
            }
            
            QUORUM_PROCESSOR->DeactivateNode(quorum->quorumID, nodeID, force);
        }
    }
}

void ConfigActivationManager::TryActivateShardServer(uint64_t nodeID, bool disregardPrevious, bool force)
{
    ConfigQuorum*       quorum;
    ConfigShardServer*  shardServer;

    if (!HEARTBEAT_MANAGER->HasHeartbeat(nodeID))
        return;

    shardServer = CONFIG_STATE->GetShardServer(nodeID);

    FOREACH (quorum, CONFIG_STATE->quorums)
    {
        Log_Trace("itQuorum->isActivatingNode: %b", quorum->isActivatingNode);

        if (quorum->isActivatingNode)
            continue;

        if (!quorum->IsInactiveMember(nodeID))
            continue;

        TryActivateShardServer(quorum, shardServer, disregardPrevious, force);
    }    
}

void ConfigActivationManager::OnExtendLease(ConfigQuorum& quorum, ClusterMessage& message)
{
    ConfigShardServer*  shardServer;

    Log_Trace();

    if (!quorum.hasPrimary || message.nodeID != quorum.primaryID)
        return;

    if (!quorum.isActivatingNode || quorum.isReplicatingActivation ||
     message.configID != quorum.configID)
    {
        Log_Trace("ExtendLease condition: %b %b %U %U", quorum.isActivatingNode,
        quorum.isReplicatingActivation,
        message.configID,
        quorum.configID);        
        return;
    }
        
    if (!quorum.isWatchingPaxosID)
    {
        Log_Message("Activating shard server %U in quorum %U: Starting to monitor the primary's paxosID",
         quorum.activatingNodeID, quorum.quorumID);
        
        quorum.OnActivationMonitoring(message.paxosID);
        configServer->OnConfigStateChanged();
    }
    else
    {
        Log_Trace();
        
        // if the primary was able to increase its paxosID, the new shardserver joined successfully
        if (message.paxosID > quorum.activationPaxosID)
        {
            Log_Message("Activating shard server %U in quorum %U: The primary was able to increase its paxosID!",
             quorum.activatingNodeID, quorum.quorumID);
            
            quorum.OnActivationReplication();
            configServer->OnConfigStateChanged();
            QUORUM_PROCESSOR->ActivateNode(quorum.quorumID, quorum.activatingNodeID);

            shardServer = CONFIG_STATE->GetShardServer(quorum.activatingNodeID);
            shardServer->tryAutoActivation = true;
        }
        else
        {
            Log_Message("Activating shard server %U in quorum %U: The primary was not able to increase its paxosID so far...",
             quorum.activatingNodeID, quorum.quorumID);
        }
    }    
}

void ConfigActivationManager::OnActivationTimeout()
{
    ConfigQuorum* quorum;
    
    Log_Trace();
    
    FOREACH (quorum, CONFIG_STATE->quorums)
    {
        if (quorum->isActivatingNode && quorum->activationExpireTime < EventLoop::Now())
        {
            ASSERT(quorum->activationExpireTime > 0);
            ASSERT(!quorum->isReplicatingActivation);

            // abort activation
            Log_Message("Activating shard server %U in quorum %U: Activation failed...",
                quorum->activatingNodeID, quorum->quorumID);

            quorum->ClearActivation();    
            configServer->OnConfigStateChanged();
        }
    }
    
    UpdateTimeout();
}

void ConfigActivationManager::UpdateTimeout()
{
    ConfigQuorum*   quorum;
    uint64_t        activationExpireTime;
    
    Log_Trace();
    
    // find lowest activationExpireTime among quorums
    activationExpireTime = 0;
    FOREACH (quorum, CONFIG_STATE->quorums)
    {
        if (quorum->isActivatingNode)
        if (!quorum->isReplicatingActivation)
        if (activationExpireTime == 0 || quorum->activationExpireTime < activationExpireTime)
            activationExpireTime = quorum->activationExpireTime;
    }
    
    if (activationExpireTime > 0)
    {
        activationTimeout.SetExpireTime(activationExpireTime);
        EventLoop::Reset(&activationTimeout);
    }
}

void ConfigActivationManager::TryActivateShardServer(ConfigQuorum* quorum, ConfigShardServer* shardServer, bool disregardPrevious, bool force)
{
    QuorumInfo* quorumInfo;

    if (force)
    {
        quorum->paxosID = 1;
        QUORUM_PROCESSOR->ActivateNode(quorum->quorumID, shardServer->nodeID);
        return;
    }

    if (!disregardPrevious && !shardServer->tryAutoActivation)
        return;

    if (!quorum->hasPrimary)
        return;

    quorumInfo = QuorumInfo::GetQuorumInfo(shardServer->quorumInfos, quorum->quorumID);
    if (!quorumInfo)
        return;
    
    if (quorum->paxosID >= quorumInfo->paxosID)
    if (quorum->paxosID - quorumInfo->paxosID > RLOG_REACTIVATION_DIFF)
        return;

    // the shard server is "almost caught up", start the activation process
    Log_Message("Activation started for shard server %U and quorum %U...",
        shardServer->nodeID, quorum->quorumID);

    quorum->OnActivationStart(shardServer->nodeID, EventLoop::Now() + ACTIVATION_TIMEOUT);
    UpdateTimeout();
        
    shardServer->tryAutoActivation = false;
    configServer->OnConfigStateChanged();
}
