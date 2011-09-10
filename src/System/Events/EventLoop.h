#ifndef EVENTLOOP_H
#define EVENTLOOP_H

#include "System/IO/IOProcessor.h"
#include "Scheduler.h"

#define SLEEP_MSEC      20
#define YIELD_TIME      (2*CLOCK_RESOLUTION)

/*
===============================================================================================

 EventLoop

===============================================================================================
*/

class EventLoop : public Scheduler
{
public: 
    static long         RunTimers();    
    static bool         RunOnce();
    static void         Run();
    static void         Init();
    static void         Shutdown();
    static uint64_t     Now();
    static void         UpdateTime();
    static void         Start();
    static void         Stop();
    static bool         IsRunning();
    static bool         IsStarted();
};

#endif
