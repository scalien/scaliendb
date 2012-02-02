#include "ConfigQuorum.h"

static bool LessThan(uint64_t a, uint64_t b)
{
    return a < b;
}

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
    quorumID = other.quorumID;
    name.Write(other.name);
    configID = other.configID;
    isActivatingNode = other.isActivatingNode;
    isWatchingPaxosID = other.isWatchingPaxosID;
    isReplicatingActivation = other.isReplicatingActivation;
    activatingNodeID = other.activatingNodeID;
    activationPaxosID = other.activationPaxosID;
    activationExpireTime = other.activationExpireTime;
    
    activeNodes = other.activeNodes;
    inactiveNodes = other.inactiveNodes;
    shards = other.shards;
    
    hasPrimary = other.hasPrimary;
    primaryID = other.primaryID;
    paxosID = other.paxosID;
    
    prev = next = this;
    
    return *this;
}

void ConfigQuorum::OnActivationStart(uint64_t nodeID, uint64_t expireTime)
{
    ASSERT(isActivatingNode == false);
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

bool ConfigQuorum::IsActiveMember(uint64_t nodeID)
{
    uint64_t* it;
    
    FOREACH (it, activeNodes)
        if (*it == nodeID)
            return true;
    
    return false;
}

bool ConfigQuorum::IsInactiveMember(uint64_t nodeID)
{
    uint64_t* it;
    
    FOREACH (it, inactiveNodes)
        if (*it == nodeID)
            return true;
    
    return false;
}

bool ConfigQuorum::IsMember(uint64_t nodeID)
{
    return IsActiveMember(nodeID) || IsInactiveMember(nodeID);
}

void ConfigQuorum::GetVolatileActiveNodes(SortedList<uint64_t>& list)
{
    list = activeNodes;
    if (isActivatingNode)
        list.Add(activatingNodeID);
}

unsigned ConfigQuorum::GetNumVolatileActiveNodes()
{
    if (!isActivatingNode)
        return activeNodes.GetLength();
    else
        return activeNodes.GetLength() + 1;
}
