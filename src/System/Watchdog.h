#ifndef WATCHDOG_H
#define WATCHDOG_H

#include "Events/Timer.h"
#include "Threading/ThreadPool.h"

/*
===============================================================================================

 Watchdog: soft watchdog for detecting long running functions

===============================================================================================
*/

class Watchdog
{
public:
    Watchdog();

    void            Start(unsigned timeout, Callable& callback);
    void            Stop();

    void            Wake();

private:
    void            ThreadFunc();

    Callable        callback;
    unsigned        timeout;
    volatile bool   flag;
    ThreadPool*     thread;
    bool            running;
};

#endif
