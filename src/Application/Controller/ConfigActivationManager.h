#ifndef CONFIGACTIVATIONMANAGER_H
#define CONFIGACTIVATIONMANAGER_H

#include "System/Events/Timer.h"
#include "Application/Common/ClusterMessage.h"

class ConfigServer; // forward

#define ACTIVATION_FAILED_PENALTY_TIME  60*60*1000  // msec, 1 hour

/*
===============================================================================================

 ConfigActivationManager

===============================================================================================
*/

class ConfigActivationManager
{
public:
    void            Init(ConfigServer* configServer);

    void            TryDeactivateShardServer(uint64_t nodeID);
    void            TryActivateShardServer(uint64_t nodeID);
    void            OnExtendLease(ConfigQuorum& quorum, ClusterMessage& message);
    
    void            OnActivationTimeout();

    void            UpdateTimeout();

private:    
    Timer           activationTimeout;
    ConfigServer*   configServer;
};

#endif
