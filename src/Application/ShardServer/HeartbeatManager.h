#ifndef HEARTBEATMANAGER_H
#define HEARTBEATMANAGER_H

#include "System/Events/Countdown.h"

#define HEARTBEAT_TIMEOUT       1000 // msec

class ShardServer;

/*
===============================================================================================

 HeartbeatManager

===============================================================================================
*/

class HeartbeatManager
{
public:
    HeartbeatManager();

    void            Init(ShardServer* shardServer);    
    void            OnHeartbeatTimeout();

private:
    Countdown       heartbeatTimeout;
    ShardServer*    shardServer;
};

#endif
