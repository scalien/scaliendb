#include "ConfigPrimaryLeaseManager.h"
#include "System/Events/EventLoop.h"
#include "Application/Common/ContextTransport.h"
#include "ConfigServer.h"
#include "ConfigQuorumProcessor.h"
#include "ConfigActivationManager.h"

void ConfigPrimaryLeaseManager::Init(ConfigServer* configServer_)
{
    configServer = configServer_;
    primaryLeaseTimeout.SetCallable(MFUNC(ConfigPrimaryLeaseManager, OnPrimaryLeaseTimeout));
}

void ConfigPrimaryLeaseManager::Shutdown()
{
    primaryLeases.DeleteList();
}

void ConfigPrimaryLeaseManager::OnPrimaryLeaseTimeout()
{
    uint64_t        now;
    ConfigState*    configState;
    ConfigQuorum*   quorum;
    ConfigShard*    shard;
    PrimaryLease*   itLease;
    
    now = EventLoop::Now();
    
    configState = configServer->GetDatabaseManager()->GetConfigState();

    shard = NULL;
    if (configState->isMigrating)
        shard = configState->GetShard(configState->migrateShardID);
    
    for (itLease = primaryLeases.First(); itLease != NULL; /* advanced in body */)
    {
        if (itLease->expireTime < now)
        {
            quorum = configState->GetQuorum(itLease->quorumID);
            
            // look for migration
            if (configState->isMigrating)
            {
                ASSERT(shard != NULL);
                
                if (configState->migrateQuorumID == quorum->quorumID || // migration dst
                 shard->quorumID == quorum->quorumID)                   // migration src
                {
                    Log_Message("Aborting shard migration...");
                    configState->isMigrating = false;
                    configState->migrateQuorumID = 0;
                    configState->migrateShardID = 0;
                    
                    configState->OnAbortShardMigration();
                }
            }
            
            if (quorum) // in case it has been deleted
            {
                quorum->hasPrimary = false;
                quorum->primaryID = 0;
            }
            itLease = primaryLeases.Delete(itLease);
        }
        else
            break;
    }

    UpdateTimer();

    configServer->OnConfigStateChanged(true);
}

void ConfigPrimaryLeaseManager::OnRequestPrimaryLease(ClusterMessage& message)
{
    ConfigQuorum*   quorum;

    if (!configServer->GetQuorumProcessor()->IsMaster())
        return;

    quorum = configServer->GetDatabaseManager()->GetConfigState()->GetQuorum(message.quorumID);
    
    if (quorum == NULL)
    {
        Log_Trace("nodeID %U requesting lease for non-existing quorum %U",
         message.nodeID, message.quorumID);
        return;
    }
    
    if (quorum->hasPrimary)
    {
        if (quorum->primaryID == message.nodeID)
            ExtendPrimaryLease(*quorum, message);
        return;
    }
    
    if (!quorum->IsActiveMember(message.nodeID))
    {
        Log_Trace("nodeID %U requesting lease but not active member or quorum %U",
         message.nodeID, message.quorumID);
        return;
    }
    
    AssignPrimaryLease(*quorum, message);
}

void ConfigPrimaryLeaseManager::AssignPrimaryLease(ConfigQuorum& quorum, ClusterMessage& message)
{
    unsigned                duration;
    PrimaryLease*           primaryLease;
    ClusterMessage          response;
    SortedList<uint64_t>    activeNodes;

    assert(quorum.hasPrimary == false);

    quorum.hasPrimary = true;
    quorum.primaryID = message.nodeID;

    primaryLease = new PrimaryLease;
    primaryLease->quorumID = quorum.quorumID;
    primaryLease->nodeID = quorum.primaryID;
    duration = MIN(message.duration, PAXOSLEASE_MAX_LEASE_TIME);
    primaryLease->expireTime = EventLoop::Now() + duration;
    primaryLeases.Add(primaryLease);

    UpdateTimer();

    quorum.GetVolatileActiveNodes(activeNodes);
    response.ReceiveLease(message.nodeID, message.quorumID,
     message.proposalID, quorum.configID, duration, false, activeNodes, quorum.shards);
    CONTEXT_TRANSPORT->SendClusterMessage(response.nodeID, response);
    
//    Log_Debug("Assigning primary lease of quorum %U to node %U with proposalID %U",
//     message.quorumID, message.nodeID, message.proposalID);
}

void ConfigPrimaryLeaseManager::ExtendPrimaryLease(ConfigQuorum& quorum, ClusterMessage& message)
{
    uint64_t                duration;
    PrimaryLease*           it;
    ClusterMessage          response;
    SortedList<uint64_t>    activeNodes;

    if (!configServer->GetQuorumProcessor()->IsMaster())
        return;

    FOREACH (it, primaryLeases)
    {
        if (it->quorumID == quorum.quorumID)
            break;
    }
    
    assert(it != NULL);
        
    primaryLeases.Remove(it);
    duration = MIN(message.duration, PAXOSLEASE_MAX_LEASE_TIME);
    it->expireTime = EventLoop::Now() + duration;
    primaryLeases.Add(it);
    UpdateTimer();

    configServer->GetActivationManager()->OnExtendLease(quorum, message);

    quorum.GetVolatileActiveNodes(activeNodes);
    response.ReceiveLease(message.nodeID, message.quorumID,
     message.proposalID, quorum.configID, duration,
     quorum.isWatchingPaxosID, activeNodes, quorum.shards);
    CONTEXT_TRANSPORT->SendClusterMessage(response.nodeID, response);

//    Log_Debug("Extending primary lease of quorum %U to node %U with proposalID %U",
//     message.quorumID, message.nodeID, message.proposalID);
}

void ConfigPrimaryLeaseManager::UpdateTimer()
{
    PrimaryLease* primaryLease;

    primaryLease = primaryLeases.First();
    if (!primaryLease)
        return;
    
    // if there is no active primary lease timeout,
    // or the timeout is later then the (new) first

    if (!primaryLeaseTimeout.IsActive() ||
     primaryLease->expireTime < primaryLeaseTimeout.GetExpireTime())
    {
        EventLoop::Remove(&primaryLeaseTimeout);
        primaryLeaseTimeout.SetExpireTime(primaryLease->expireTime);
        EventLoop::Add(&primaryLeaseTimeout);
    }
}

PrimaryLease::PrimaryLease()
{
    prev = next = this;
    quorumID = 0;
    nodeID = 0;
    expireTime = 0;
}
