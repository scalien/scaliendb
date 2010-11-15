#include "ConfigPrimaryLeaseManager.h"
#include "System/Events/EventLoop.h"
#include "Application/Common/ContextTransport.h"
#include "Controller.h"

void ConfigPrimaryLeaseManager::Init(Controller* controller_)
{
    controller = controller_;
    primaryLeaseTimeout.SetCallable(MFUNC(ConfigPrimaryLeaseManager, OnPrimaryLeaseTimeout));
}

void ConfigPrimaryLeaseManager::OnPrimaryLeaseTimeout()
{
    uint64_t        now;
    ConfigQuorum*   quorum;
    PrimaryLease*   itLease;
    
    now = EventLoop::Now();

    for (itLease = primaryLeases.First(); itLease != NULL; /* advanced in body */)
    {
        if (itLease->expireTime < now)
        {
            quorum = controller->GetConfigState()->GetQuorum(itLease->quorumID);
            quorum->hasPrimary = false;
            quorum->primaryID = 0;
            itLease = primaryLeases.Delete(itLease);
        }
        else
            break;
    }

    UpdateTimer();

    controller->OnConfigStateChanged(); // UpdateListeners();
}

void ConfigPrimaryLeaseManager::OnRequestPrimaryLease(ClusterMessage& message)
{
    uint64_t*       it;
    ConfigQuorum*   quorum;

    quorum = controller->GetConfigState()->GetQuorum(message.quorumID);
    
    if (quorum == NULL)
    {
        Log_Trace("nodeID %" PRIu64 " requesting lease for non-existing quorum %" PRIu64 "",
         message.nodeID, message.quorumID);
        return;
    }
    
    if (quorum->hasPrimary)
    {
        if (quorum->primaryID == message.nodeID)
            ExtendPrimaryLease(*quorum, message);
        return;
    }
    
    for (it = quorum->activeNodes.First(); it != NULL; it = quorum->activeNodes.Next(it))
    {
        if (*it == message.nodeID)
            break;
    }
    
    if (it == NULL)
    {
        Log_Trace("nodeID %" PRIu64 " requesting lease but not active member or quorum %" PRIu64 "",
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
    ConfigQuorum::NodeList  activeNodes;

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

    activeNodes = quorum.GetVolatileActiveNodes();
    response.ReceiveLease(message.nodeID, message.quorumID,
     message.proposalID, duration, activeNodes);
    CONTEXT_TRANSPORT->SendClusterMessage(response.nodeID, response);
}

void ConfigPrimaryLeaseManager::ExtendPrimaryLease(ConfigQuorum& quorum, ClusterMessage& message)
{
    bool                    found;
    unsigned                duration;
    PrimaryLease*           it;
    ConfigMessage*          itConfigMessage;
    ClusterMessage          response;
    ConfigQuorum::NodeList  activeNodes;

    duration = MIN(message.duration, PAXOSLEASE_MAX_LEASE_TIME);

    FOREACH(it, primaryLeases)
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

    activeNodes = quorum.GetVolatileActiveNodes();
    response.ReceiveLease(message.nodeID, message.quorumID,
     message.proposalID, duration, activeNodes);
    CONTEXT_TRANSPORT->SendClusterMessage(response.nodeID, response);

    controller->OnLearnPrimaryLease();

    // keep track of paxosID of shard server
    // if it's able to increase it, the new shard server has successfully joined the quorum
    if (quorum.isActivatingNode &&
     !quorum.isReplicatingActivation && message.configID == quorum.configID)
    {
        if (!quorum.isWatchingPaxosID)
        {
            // start monitoring its paxosID
            quorum.activationPaxosID = message.paxosID;
            quorum.isWatchingPaxosID = true;
        }
        else
        {
            if (message.paxosID > quorum.activationPaxosID)
            {
                quorum.isWatchingPaxosID = false;
                // node successfully joined the quorum, tell other Controllers!
                quorum.isReplicatingActivation = true;
                quorum.activationExpireTime = 0;
                UpdateActivationTimeout();

                // make sure there is no corresponding activate message pending
                found = false;
                FOREACH(itConfigMessage, configMessages)
                {
                    if (itConfigMessage->type == CONFIGMESSAGE_ACTIVATE_SHARDSERVER &&
                     itConfigMessage->quorumID == quorum.quorumID &&
                     itConfigMessage->nodeID == quorum.activatingNodeID)
                    {
                        found = true;
                        break;
                    }
                }
                if (found)
                    return;
                    
                itConfigMessage = new ConfigMessage();
                itConfigMessage->fromClient = false;
                itConfigMessage->ActivateShardServer(quorum.quorumID, quorum.activatingNodeID);
                configMessages.Append(itConfigMessage);
                TryAppend();
            }
        }    
    }
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

//PrimaryLease::PrimaryLease()
//{
//    prev = next = this;
//    quorumID = 0;
//    nodeID = 0;
//    expireTime = 0;
//}
