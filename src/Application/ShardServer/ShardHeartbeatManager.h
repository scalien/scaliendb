#ifndef SHARDHEARTBEATMANAGER_H
#define SHARDHEARTBEATMANAGER_H

#include "System/Events/Countdown.h"

#define NORMAL_HEARTBEAT_TIMEOUT            1000 // msec
#define ACTIVATION_HEARTBEAT_TIMEOUT        100  // msec

class ShardServer;

/*
===============================================================================================

 ShardHeartbeatManager

===============================================================================================
*/

class ShardHeartbeatManager
{
public:
    ShardHeartbeatManager();

    void            Init(ShardServer* shardServer);    
    void            OnHeartbeatTimeout();
    void            OnActivation();
    void            OnActivationTimeout();

private:
    ShardServer*    shardServer;
    Countdown       heartbeatTimeout;
    Countdown       activationTimeout;
};

#endif
