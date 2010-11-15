#ifndef CONFIGACTIVATIONMANAGER_H
#define CONFIGACTIVATIoNMANAGER_H

#include "System/Events/Timer.h"

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

    void            TryDeactivateShardServer(uint64_t nodeID);
    void            TryActivateShardServer(uint64_t nodeID);
    
    void            OnActivationTimeout();

private:
    void            UpdateTimeout();
    
    Timer           activationTimeout;
    Controller*     controller;
};

#endif
