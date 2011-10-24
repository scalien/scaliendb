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

    void            TryDeactivateShardServer(uint64_t nodeID, bool force = false);
    void            TryActivateShardServer(uint64_t nodeID, bool disregardPrevious = true, bool force = false);
    void            OnExtendLease(ConfigQuorum& quorum, ClusterMessage& message);
    
    void            OnActivationTimeout();

    void            UpdateTimeout();

private:    
    Timer           activationTimeout;
    ConfigServer*   configServer;
};

#endif
