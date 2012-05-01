#include "ConfigPrimaryLeaseManager.h"
#include "System/Events/EventLoop.h"
#include "Application/Common/ContextTransport.h"
#include "ConfigServer.h"
#include "ConfigQuorumProcessor.h"
#include "ConfigActivationManager.h"

#define CONFIG_STATE (configServer->GetDatabaseManager()->GetConfigState())

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
    ConfigQuorum*   quorum;
    ConfigShard*    shard;
    PrimaryLease*   primaryLease;
    
    now = EventLoop::Now();
    
    shard = NULL;
    if (CONFIG_STATE->isMigrating)
        shard = CONFIG_STATE->GetShard(CONFIG_STATE->migrateSrcShardID);
    
    for (primaryLease = primaryLeases.First(); primaryLease != NULL; /* advanced in body */)
    {
        if (primaryLease->expireTime < now)
        {
            quorum = CONFIG_STATE->GetQuorum(primaryLease->quorumID);

            Log_Message("Node %U lost the primary lease for quorum %U",
             primaryLease->nodeID, primaryLease->quorumID);
            
            // look for migration
            if (CONFIG_STATE->isMigrating)
            {
                ASSERT(shard != NULL);
                
                if (CONFIG_STATE->migrateQuorumID == quorum->quorumID || // migration dst
                 shard->quorumID == quorum->quorumID)                    // migration src
                {
                    Log_Message("Aborting shard migration...");                    
                    CONFIG_STATE->OnAbortShardMigration();
                }
            }
            
            if (quorum) // in case it has been deleted
            {
                quorum->hasPrimary = false;
                quorum->primaryID = 0;
            }
            primaryLease = primaryLeases.Delete(primaryLease);            
        }
        else
            break;
    }

    UpdateTimer();

    configServer->OnConfigStateChanged();
}

void ConfigPrimaryLeaseManager::OnRequestPrimaryLease(ClusterMessage& message)
{
    uint64_t            priority;
    uint64_t*           itNodeID;
    ConfigQuorum*       quorum;
    ConfigShardServer*  shardServer;
    PrimaryLease*       primaryLease;

    Log_Trace("nodeID %U requesting lease %U for quorum %U",
        message.nodeID, message.proposalID, message.quorumID);

    if (!configServer->GetQuorumProcessor()->IsMaster())
        return;

    quorum = configServer->GetDatabaseManager()->GetConfigState()->GetQuorum(message.quorumID);
    
    if (quorum == NULL)
    {
        Log_Trace("nodeID %U requesting lease for non-existing quorum %U",
         message.nodeID, message.quorumID);
        return;
    }
    
    if (!quorum->IsActiveMember(message.nodeID))
    {
        Log_Trace("nodeID %U requesting lease but not active member of quorum %U",
         message.nodeID, message.quorumID);
        return;
    }

    shardServer = CONFIG_STATE->GetShardServer(message.nodeID);
    priority = shardServer->GetQuorumPriority(message.quorumID);
    if (priority == 0)
    {
        Log_Trace("nodeID %U requesting lease but priority is 0 in quorum %U",
         message.nodeID, message.quorumID);
        return;
    }
    FOREACH(itNodeID, quorum->activeNodes)
    {
        if (!configServer->GetHeartbeatManager()->HasHeartbeat(*itNodeID))
            continue;
        shardServer = CONFIG_STATE->GetShardServer(*itNodeID);
        if (shardServer->GetQuorumPriority(message.quorumID) > priority)
            return; // a higher priority shard server is active
    }

    FOREACH(primaryLease, primaryLeases)
    {
        if (primaryLease->quorumID == message.quorumID && primaryLease->nodeID != message.nodeID)
        {
            Log_Trace("nodeID %U requesting lease for quorum %U, "
             "but found non-expired lease for that quorum for node %U",
             message.nodeID, message.quorumID, primaryLease->nodeID);
            return;
        }
    }

    if (quorum->hasPrimary)
    {
        if (quorum->primaryID == message.nodeID)
            ExtendPrimaryLease(*quorum, message);
    }
    else
        AssignPrimaryLease(*quorum, message);
}

unsigned ConfigPrimaryLeaseManager::GetNumPrimaryLeases()
{
    return primaryLeases.GetLength();
}

void ConfigPrimaryLeaseManager::AssignPrimaryLease(ConfigQuorum& quorum, ClusterMessage& message)
{
    unsigned                duration;
    uint64_t*               itNodeID;
    PrimaryLease*           primaryLease;
    ClusterMessage          response;
    SortedList<uint64_t>    activeNodes;

    ASSERT(quorum.hasPrimary == false);

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

    FOREACH(itNodeID, activeNodes)
        CONTEXT_TRANSPORT->SendClusterMessage(*itNodeID, response);
    FOREACH(itNodeID, quorum.inactiveNodes)
        CONTEXT_TRANSPORT->SendClusterMessage(*itNodeID, response);
    
    Log_Message("Assigning primary lease of quorum %U to node %U with proposalID %U",
     message.quorumID, message.nodeID, message.proposalID);
}

void ConfigPrimaryLeaseManager::ExtendPrimaryLease(ConfigQuorum& quorum, ClusterMessage& message)
{
    uint64_t                duration;
    uint64_t*               itNodeID;
    PrimaryLease*           primaryLease;
    ClusterMessage          response;
    SortedList<uint64_t>    activeNodes;

    FOREACH (primaryLease, primaryLeases)
    {
        if (primaryLease->quorumID == quorum.quorumID)
            break;
    }
    
    ASSERT(primaryLease != NULL);
        
    primaryLeases.Remove(primaryLease);
    duration = MIN(message.duration, PAXOSLEASE_MAX_LEASE_TIME);
    primaryLease->expireTime = EventLoop::Now() + duration;
    primaryLeases.Add(primaryLease);
    UpdateTimer();

    configServer->GetActivationManager()->OnExtendLease(quorum, message);

    quorum.GetVolatileActiveNodes(activeNodes);
    response.ReceiveLease(message.nodeID, message.quorumID,
     message.proposalID, quorum.configID, duration,
     quorum.isWatchingPaxosID, activeNodes, quorum.shards);

    FOREACH(itNodeID, activeNodes)
        CONTEXT_TRANSPORT->SendClusterMessage(*itNodeID, response);
    FOREACH(itNodeID, quorum.inactiveNodes)
        CONTEXT_TRANSPORT->SendClusterMessage(*itNodeID, response);
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
