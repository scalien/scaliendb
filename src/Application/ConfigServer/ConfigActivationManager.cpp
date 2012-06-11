#include "ConfigActivationManager.h"
#include "System/Events/EventLoop.h"
#include "System/Config.h"
#include "Application/Common/DatabaseConsts.h"
#include "Application/Common/ContextTransport.h"
#include "ConfigServer.h"
#include "ConfigQuorumProcessor.h"

#define CONFIG_STATE            (configServer->GetDatabaseManager()->GetConfigState())
#define QUORUM_PROCESSOR        (configServer->GetQuorumProcessor())
#define HEARTBEAT_MANAGER       (configServer->GetHeartbeatManager())

void ConfigActivationManager::Init(ConfigServer* configServer_)
{
    configServer = configServer_;
    
    numRoundsTriggerActivation = configFile.GetIntValue("numRoundsTriggerActivation", RLOG_REACTIVATION_DIFF);
    activationTimeoutInterval = configFile.GetIntValue("activationTimeout", ACTIVATION_TIMEOUT);

    activationTimeout.SetCallable(MFUNC(ConfigActivationManager, OnActivationTimeout));
    nextActivationTimeout.SetCallable(MFUNC(ConfigActivationManager, OnNextActivationTimeout));
}

void ConfigActivationManager::SetNumRoundsTriggerActivation(uint64_t numRoundsTriggerActivation_)
{
    numRoundsTriggerActivation = numRoundsTriggerActivation_;
}

uint64_t ConfigActivationManager::GetNumRoundsTriggerActivation()
{
    return numRoundsTriggerActivation;
}

void ConfigActivationManager::SetActivationTimeout(uint64_t activationTimeoutInterval_)
{
    activationTimeoutInterval = activationTimeoutInterval_;
}

uint64_t ConfigActivationManager::GetActivationTimeout()
{
    return activationTimeoutInterval;
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
            UpdateTimeouts();

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
                UpdateTimeouts();

                Log_Message("Activation failed for shard server %U and quorum %U...",
                 quorum->activatingNodeID, quorum->quorumID);
            }
            
            QUORUM_PROCESSOR->DeactivateNode(quorum->quorumID, nodeID, force);
        }
    }

    shardServer = CONFIG_STATE->GetShardServer(nodeID);
    if (shardServer)
    {
        // if shard server loses heartbeat because it is restarted
        // reset its activation state so it can come back quickly after restart
        shardServer->activationPhase = 0;
        shardServer->nextActivationTime = 0;
        UpdateTimeouts();
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

        // HACK: the shard server primary uses this to signal to the controller that activation was successful
        if (message.duration == (PAXOSLEASE_MAX_LEASE_TIME - 1))
        {
            Log_Message("Activating shard server %U in quorum %U: The primary was able to increase its paxosID!",
             quorum.activatingNodeID, quorum.quorumID);
            
            quorum.OnActivationReplication();
            configServer->OnConfigStateChanged();
            QUORUM_PROCESSOR->ActivateNode(quorum.quorumID, quorum.activatingNodeID);

            shardServer = CONFIG_STATE->GetShardServer(quorum.activatingNodeID);
            shardServer->activationPhase = 0;
            shardServer->nextActivationTime = 0;

            UpdateTimeouts(); // remove timeout
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
    
    UpdateTimeouts();
}

void ConfigActivationManager::OnNextActivationTimeout()
{
    ConfigShardServer*  shardServer;
    uint64_t            now;

    now = EventLoop::Now();
    FOREACH(shardServer, CONFIG_STATE->shardServers)
    {
        if (shardServer->activationPhase <= 0)
            continue;
        ASSERT(shardServer->nextActivationTime > 0);
        if (shardServer->nextActivationTime < now)
        {
            TryActivateShardServer(shardServer->nodeID);
            return;
        }
    }
}

void ConfigActivationManager::UpdateTimeouts()
{
    UpdateActivationTimeout();
    UpdateNextActivationTimeout();
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

    if (disregardPrevious == false && shardServer->activationPhase == -1)
        return;
    
    if (disregardPrevious)
    {
        shardServer->activationPhase = 0;
        shardServer->nextActivationTime = 0;
    }

    if (!quorum->hasPrimary)
        return;

    if (shardServer->activationPhase > 0)
    {
        ASSERT(shardServer->nextActivationTime > 0);
        if (EventLoop::Now() < shardServer->nextActivationTime)
            return; // wait for next timer event
    }


    quorumInfo = QuorumInfo::GetQuorumInfo(shardServer->quorumInfos, quorum->quorumID);
    if (!quorumInfo)
        return;
    
    if (quorum->paxosID >= quorumInfo->paxosID)
    if (quorum->paxosID - quorumInfo->paxosID > numRoundsTriggerActivation)
        return;

    // the shard server is "almost caught up", start the activation process
    Log_Message("Activation started for shard server %U and quorum %U...",
        shardServer->nodeID, quorum->quorumID);

    quorum->OnActivationStart(shardServer->nodeID, EventLoop::Now() + activationTimeoutInterval);
    
    shardServer->activationPhase++;
    shardServer->nextActivationTime = EventLoop::Now() + activationTimeoutInterval * pow(2.0, shardServer->activationPhase);

    configServer->OnConfigStateChanged();
}

void ConfigActivationManager::UpdateActivationTimeout()
{
    ConfigQuorum*   quorum;
    uint64_t        activationExpireTime;
    
    Log_Trace();
    
    // find lowest activationExpireTime among quorums
    activationExpireTime = 0;
    FOREACH (quorum, CONFIG_STATE->quorums)
    {
        if (quorum->isActivatingNode &&
            !quorum->isReplicatingActivation &&
            (activationExpireTime == 0 || quorum->activationExpireTime < activationExpireTime))
                activationExpireTime = quorum->activationExpireTime;
    }
    
    if (activationTimeout.IsActive())
        EventLoop::Remove(&activationTimeout);

    if (activationExpireTime > 0)
    {
        activationTimeout.SetExpireTime(activationExpireTime);
        EventLoop::Add(&activationTimeout);
    }
}

void ConfigActivationManager::UpdateNextActivationTimeout()
{
    uint64_t            nextActivationTime;
    ConfigShardServer*  shardServer;

    nextActivationTime = 0;
    FOREACH(shardServer, CONFIG_STATE->shardServers)
    {
        if (shardServer->activationPhase > 0)
        {
            ASSERT(shardServer->nextActivationTime > 0);
            if (nextActivationTime == 0 || shardServer->nextActivationTime < nextActivationTime)
                nextActivationTime = shardServer->nextActivationTime;
        }
    }

    if (nextActivationTimeout.IsActive())
        EventLoop::Remove(&nextActivationTimeout);

    if (nextActivationTime > 0)
    {
        nextActivationTimeout.SetExpireTime(nextActivationTime);
        EventLoop::Add(&nextActivationTimeout);
    }
}
