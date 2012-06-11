#ifndef CONFIGACTIVATIONMANAGER_H
#define CONFIGACTIVATIONMANAGER_H

#include "System/Events/Timer.h"
#include "Application/Common/ClusterMessage.h"

class ConfigServer; // forward

/*
===============================================================================================

 ConfigActivationManager

===============================================================================================
*/

class ConfigActivationManager
{
public:
    void            Init(ConfigServer* configServer);

    void            SetNumRoundsTriggerActivation(uint64_t numRoundsTriggerActivation_);
    uint64_t        GetNumRoundsTriggerActivation();
    
    void            SetActivationTimeout(uint64_t activationTimeoutInterval_);
    uint64_t        GetActivationTimeout();

    void            TryDeactivateShardServer(uint64_t nodeID, bool force = false);
    void            TryActivateShardServer(uint64_t nodeID, bool disregardPrevious = true, bool force = false);
    void            OnExtendLease(ConfigQuorum& quorum, ClusterMessage& message);
    
    void            OnActivationTimeout();
    void            OnNextActivationTimeout();

    void            UpdateTimeouts();

private:
    void            TryActivateShardServer(ConfigQuorum* quorum, ConfigShardServer* shardServer, bool disregardPrevious, bool force);
    void            UpdateActivationTimeout();
    void            UpdateNextActivationTimeout();

    Timer           activationTimeout;
    Timer           nextActivationTimeout;
    ConfigServer*   configServer;
    uint64_t        numRoundsTriggerActivation;
    uint64_t        activationTimeoutInterval;
};

#endif
