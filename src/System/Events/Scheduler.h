#ifndef SCHEDULER_H
#define SCHEDULER_H

#include "System/Containers/InSortedList.h"
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
};

#endif
