#ifndef CONFIGHEARTBEATMANAGER_H
#define CONFIGHEARTBEATMANAGER_H

#include "System/Containers/InSortedList.h"
#include "System/Events/Countdown.h"
#include "Application/Common/ClusterMessage.h"

class Heartbeat;    // forward
class Controller;   // forward

/*
===============================================================================================

 ConfigHeartbeatManager

===============================================================================================
*/

class ConfigHeartbeatManager
{
    typedef InSortedList<Heartbeat>     HeartbeatList;

public:
    void                Init(Controller* controller);
    
    void                OnHeartbeatMessage(ClusterMessage& message);
    void                OnHeartbeatTimeout();

    bool                HasHeartbeat(uint64_t nodeID);

private:
    void                RegisterHeartbeat(uint64_t nodeID);

    Controller*         controller;
    HeartbeatList       heartbeats;
    Countdown           heartbeatTimeout;
};

/*
===============================================================================================

 Heartbeat

===============================================================================================
*/

class Heartbeat
{
public:

    Heartbeat()     { prev = next = this; }

    uint64_t        nodeID;
    uint64_t        expireTime;
    
    Heartbeat*      prev;
    Heartbeat*      next;
};

inline bool LessThan(Heartbeat &a, Heartbeat &b)
{
    return (a.expireTime < b.expireTime);
}

#endif
