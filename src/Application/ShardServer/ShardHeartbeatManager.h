#ifndef SHARDHEARTBEATMANAGER_H
#define SHARDHEARTBEATMANAGER_H

#include "System/Events/Countdown.h"

#define HEARTBEAT_TIMEOUT       1000 // msec

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

private:
    Countdown       heartbeatTimeout;
    ShardServer*    shardServer;
};

#endif
