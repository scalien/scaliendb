#include "ConfigQuorum.h"

ConfigQuorum::ConfigQuorum()
{
    prev = next = this;
    quorumID = 0;
    configID = 0;
    hasPrimary = false;
    primaryID = 0;
    paxosID = 0;
    ClearActivation();
}

ConfigQuorum::ConfigQuorum(const ConfigQuorum& other)
{
    *this = other;
}

ConfigQuorum& ConfigQuorum::operator=(const ConfigQuorum& other)
{
    uint64_t*   sit;
    
    quorumID = other.quorumID;
    configID = other.configID;
    isActivatingNode = other.isActivatingNode;
    isWatchingPaxosID = other.isWatchingPaxosID;
    isReplicatingActivation = other.isReplicatingActivation;
    activatingNodeID = other.activatingNodeID;
    activationPaxosID = other.activationPaxosID;
    activationExpireTime = other.activationExpireTime;
    
    activeNodes = other.activeNodes;
    inactiveNodes = other.inactiveNodes;
    
    for (sit = other.shards.First(); sit != NULL; sit = other.shards.Next(sit))
        shards.Append(*sit);
    
    hasPrimary = other.hasPrimary;
    primaryID = other.primaryID;
    paxosID = other.paxosID;
    
    prev = next = this;
    
    return *this;
}

void ConfigQuorum::OnActivationStart(uint64_t nodeID, uint64_t expireTime)
{
    assert(isActivatingNode == false);
    isActivatingNode = true;
    activatingNodeID = nodeID;
    isWatchingPaxosID = false;
    isReplicatingActivation = false;
    configID++;
    activationExpireTime = expireTime;
}

void ConfigQuorum::OnActivationMonitoring(uint64_t paxosID)
{
    isWatchingPaxosID = true;
    activationPaxosID = paxosID;
}

void ConfigQuorum::OnActivationReplication()
{
    isWatchingPaxosID = false;
    isReplicatingActivation = true;
    activationExpireTime = 0;
}

void ConfigQuorum::ClearActivation()
{
    isActivatingNode = false;
    isWatchingPaxosID = false;
    isReplicatingActivation = false;
    activatingNodeID = 0;
    activationPaxosID = 0;
    activationExpireTime = 0;
}

ConfigQuorum::NodeList ConfigQuorum::GetVolatileActiveNodes()
{
    NodeList list;
    
    list = activeNodes;
    if (isActivatingNode)
        list.Append(activatingNodeID);

    return list;
}
