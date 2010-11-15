#ifndef CONFIGACTIVATIONMANAGER_H
#define CONFIGACTIVATIONMANAGER_H

#include "System/Events/Timer.h"
#include "Application/Common/ClusterMessage.h"

class Controller; // forward

/*
===============================================================================================

 ConfigActivationManager

===============================================================================================
*/

class ConfigActivationManager
{
public:
    void            Init(Controller* controller);
    void            Shutdown();

    void            TryDeactivateShardServer(uint64_t nodeID);
    void            TryActivateShardServer(uint64_t nodeID);
    void            OnExtendLease(ConfigQuorum& quorum, ClusterMessage& message);
    
    void            OnActivationTimeout();

private:
    void            UpdateTimeout();
    
    Timer           activationTimeout;
    Controller*     controller;
};

#endif
