#ifndef SCHEDULER_H
#define SCHEDULER_H

#include "System/Containers/InSortedList.h"
#include "System/Threading/Mutex.h"
#include "Timer.h"

/*
===============================================================================================

 Scheduler

===============================================================================================
*/
 
class Scheduler
{
public:
    static void                 Add(Timer* timer);
    static void                 Remove(Timer* timer);
    static void                 Reset(Timer* timer);
    static void                 Shutdown();

    static unsigned             GetNumTimers();

protected:
    static InSortedList<Timer>  timers;
#ifdef EVENTLOOP_MULTITHREADED
    static Mutex                mutex;
#endif

    static void                 UnprotectedAdd(Timer* timer);
    static void                 UnprotectedRemove(Timer* timer);
};

#endif
