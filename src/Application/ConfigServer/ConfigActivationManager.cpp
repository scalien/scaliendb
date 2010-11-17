#include "ConfigActivationManager.h"
#include "System/Events/EventLoop.h"
#include "Application/Common/ContextTransport.h"
#include "ConfigServer.h"
#include "ConfigQuorumProcessor.h"

void ConfigActivationManager::Init(ConfigServer* configServer_)
{
    configServer = configServer_;
    
    activationTimeout.SetCallable(MFUNC(ConfigActivationManager, OnActivationTimeout));
}

void ConfigActivationManager::TryDeactivateShardServer(uint64_t nodeID)
{
    uint64_t*           itNodeID;
    ConfigState*        configState;
    ConfigQuorum*       itQuorum;
    ConfigShardServer*  shardServer;

    configState = configServer->GetDatabaseManager()->GetConfigState();

    FOREACH(itQuorum, configState->quorums)
    {
        // don't deactivate the last node due to TotalPaxos semantics
        // otherwise, we wouldn't know which one to bring back, which is up-to-date
        if (itQuorum->activeNodes.GetLength() <= 1)
            continue;

        if (itQuorum->isActivatingNode && itQuorum->activatingNodeID == nodeID)
        {
            shardServer = configState->GetShardServer(itQuorum->activatingNodeID);
            assert(shardServer != NULL);
            shardServer->nextActivationTime = EventLoop::Now() + ACTIVATION_FAILED_PENALTY_TIME;

            itQuorum->ClearActivation();
            UpdateTimeout();

            Log_Message("Activation failed for quorum %U and shard server %U",
             itQuorum->quorumID, itQuorum->activatingNodeID);
        }

        FOREACH(itNodeID, itQuorum->activeNodes)
        {
            if (*itNodeID == nodeID)
            {
                // if this node was part of an activation process, cancel it
                if (itQuorum->isActivatingNode)
                {
                    shardServer = configState->GetShardServer(itQuorum->activatingNodeID);
                    assert(shardServer != NULL);
                    shardServer->nextActivationTime =
                     EventLoop::Now() + ACTIVATION_FAILED_PENALTY_TIME;

                    itQuorum->ClearActivation();
                    UpdateTimeout();

                    Log_Message("Activation failed for quorum %U and shard server %U",
                     itQuorum->quorumID, itQuorum->activatingNodeID);
                }
                
                configServer->GetQuorumProcessor()->DeactivateNode(itQuorum->quorumID, nodeID);
            }
        }
    }
}

void ConfigActivationManager::TryActivateShardServer(uint64_t nodeID)
{
    uint64_t            paxosID;
    uint64_t*           itNodeID;
    ConfigState*        configState;
    ConfigQuorum*       itQuorum;
    ConfigShardServer*  shardServer;
    uint64_t            now;

    configState = configServer->GetDatabaseManager()->GetConfigState();
    shardServer = configState->GetShardServer(nodeID);

    now = EventLoop::Now();
    if (now < shardServer->nextActivationTime)
    {
        Log_Trace("not trying activation due to penalty");
        return;
    }

    FOREACH(itQuorum, configState->quorums)
    {
        if (itQuorum->isActivatingNode)
            continue;
            
        if (!itQuorum->hasPrimary)
            continue;

        FOREACH(itNodeID, itQuorum->inactiveNodes)
        {
            if (*itNodeID == nodeID)
            {
                paxosID = QuorumPaxosID::GetPaxosID(shardServer->quorumPaxosIDs, itQuorum->quorumID);
                if (paxosID >= (itQuorum->paxosID - RLOG_REACTIVATION_DIFF))
                {
                    // the shard server is "almost caught up", start the activation process
                    itQuorum->isActivatingNode = true;
                    itQuorum->activatingNodeID = nodeID;
                    itQuorum->isWatchingPaxosID = false;
                    itQuorum->isReplicatingActivation = false;
                    itQuorum->configID++;
                    itQuorum->activationExpireTime = now + PAXOSLEASE_MAX_LEASE_TIME;
                    UpdateTimeout();

                    Log_Message("Activation started for quorum %U and shard server %U",
                     itQuorum->quorumID, itQuorum->activatingNodeID);
                }
            }
        }
    }    
}

void ConfigActivationManager::OnExtendLease(ConfigQuorum& quorum, ClusterMessage& message)
{
    if (!quorum.isActivatingNode || quorum.isReplicatingActivation ||
     message.configID != quorum.configID)
        return;


    if (!quorum.isWatchingPaxosID)
    {
        // start monitoring the primary's paxosID
        quorum.isWatchingPaxosID = true;
        quorum.activationPaxosID = message.paxosID;
        configServer->OnConfigStateChanged();
    }
    else
    {
        // if the primary was able to increase its paxosID, the new shardserver joined successfully
        if (message.paxosID > quorum.activationPaxosID)
        {
            quorum.isWatchingPaxosID = false;
            quorum.isReplicatingActivation = true;
            quorum.activationExpireTime = 0;
            configServer->OnConfigStateChanged();

            configServer->GetQuorumProcessor()->ActivateNode(quorum.quorumID, quorum.activatingNodeID);
        }
    }    
}

void ConfigActivationManager::OnActivationTimeout()
{
    uint64_t            now;
    ConfigState*        configState;
    ConfigQuorum*       itQuorum;
    ConfigShardServer*  shardServer;
    
    Log_Trace();
    
    now = EventLoop::Now();
    configState = configServer->GetDatabaseManager()->GetConfigState();
    
    FOREACH(itQuorum, configState->quorums)
    {
        if (itQuorum->activationExpireTime > 0 && itQuorum->activationExpireTime < now)
        {
            // stop activation
            
            assert(itQuorum->isActivatingNode == true);
            assert(itQuorum->isReplicatingActivation == false);

            shardServer = configState->GetShardServer(itQuorum->activatingNodeID);
            assert(shardServer != NULL);
            shardServer->nextActivationTime = now + ACTIVATION_FAILED_PENALTY_TIME;

            itQuorum->isActivatingNode = false;
            itQuorum->isWatchingPaxosID = false;
            itQuorum->isReplicatingActivation = false;
            itQuorum->activatingNodeID = 0;
            itQuorum->activationPaxosID = 0;
            itQuorum->activationExpireTime = 0;
            
            Log_Message("Activation failed for quorum %U and shard server %U",
             itQuorum->quorumID, itQuorum->activatingNodeID);
             
            configServer->OnConfigStateChanged();
        }
    }
    
    UpdateTimeout();
}

void ConfigActivationManager::UpdateTimeout()
{
    ConfigQuorum*   it;
    uint64_t        activationExpireTime;
    
    Log_Trace();
    
    activationExpireTime = 0;
    FOREACH(it, configServer->GetDatabaseManager()->GetConfigState()->quorums)
    {
        if (it->isActivatingNode && !it->isReplicatingActivation)
        {
            if (activationExpireTime == 0 || it->activationExpireTime < activationExpireTime)
                activationExpireTime = it->activationExpireTime;
        }
    }
    
    if (activationExpireTime > 0)
    {
        activationTimeout.SetExpireTime(activationExpireTime);
        EventLoop::Reset(&activationTimeout);
    }
}
