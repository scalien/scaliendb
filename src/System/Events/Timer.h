#ifndef TIMER_H
#define TIMER_H

#include "System/Time.h"
#include "Callable.h"

/*
===============================================================================================

 Timer

===============================================================================================
*/

class Timer
{
    friend class Scheduler;

public:
    Timer();
    virtual ~Timer();
        
    void            SetCallable(Callable callable);
    void            SetExpireTime(uint64_t when);

    uint64_t        GetExpireTime();
    bool            IsActive();
    
    void            Execute();
    
    virtual void    OnAdd() {};
    
    Timer*          next;
    Timer*          prev;

protected:
    bool            active;
    uint64_t        expireTime;
    Callable        callable;
};

inline bool LessThan(Timer& a, Timer& b)
{
    return (a.GetExpireTime() < b.GetExpireTime());
}

#endif
