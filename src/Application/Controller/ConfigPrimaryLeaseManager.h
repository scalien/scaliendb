#ifndef CONFIGPRIMARYLEASEMANAGER_H
#define CONFIGPRIMARYLEASEMANAGER_H

#include "System/Containers/InSortedList.h"
#include "System/Events/Timer.h"
#include "Application/Common/ClusterMessage.h"

class PrimaryLease; // forward
class Controller;   // forward

/*
===============================================================================================

 ConfigPrimaryLeaseManager

===============================================================================================
*/

class ConfigPrimaryLeaseManager
{
    typedef InSortedList<PrimaryLease>  LeaseList;

public:
    void            Init(Controller* controller);

    void            OnPrimaryLeaseTimeout();
    void            OnRequestPrimaryLease(ClusterMessage& message);
    
private:
    void            AssignPrimaryLease(ConfigQuorum& quorum, ClusterMessage& message);
    void            ExtendPrimaryLease(ConfigQuorum& quorum, ClusterMessage& message);
    
    void            UpdateTimer();

    Controller*     controller;
    LeaseList       primaryLeases;
    Timer           primaryLeaseTimeout;
};

/*
===============================================================================================

 PrimaryLease

===============================================================================================
*/

class PrimaryLease
{
public:
    PrimaryLease();

    uint64_t        quorumID;
    uint64_t        nodeID;
    uint64_t        expireTime;
    
    PrimaryLease*   prev;
    PrimaryLease*   next;
};

inline bool LessThan(PrimaryLease &a, PrimaryLease &b)
{
    return (a.expireTime < b.expireTime);
}

#endif
