#include "EventLoop.h"

static volatile uint64_t    now;
static bool                 running;

long EventLoop::RunTimers()
{
    Timer*      timer;
    Timer*      previous;
    bool        noWait;
    
    noWait = false;
    previous = NULL;
    for (timer = timers.First(); timer != NULL; )
    {
        UpdateTime();
        if (timer->GetExpireTime() <= now)
        {
            if (timer == previous && timer->GetExpireTime() == 0)
            {
                timer = timers.Next(timer);
                previous = timer;
                noWait = true;
                continue;
            }
            Remove(timer);
            timer->Execute();
            previous = timer;
            timer = timers.First();
        }
        else
        {
            if (noWait)
                return 0;
            return timer->GetExpireTime() - now;
        }
    }

    if (noWait)
        return 0;
    
    return -1; // no timers to wait for
}

bool EventLoop::RunOnce()
{
    long sleep;

    sleep = RunTimers();
    
    if (sleep < 0)
        sleep = SLEEP_MSEC;
    
    return IOProcessor::Poll(sleep);
}

void EventLoop::Run()
{
    running = true;
    while(running)
        if (!RunOnce())
            break;
}

void EventLoop::Init()
{
}

void EventLoop::Shutdown()
{
    Scheduler::Shutdown();
}

uint64_t EventLoop::Now()
{
    if (now == 0)
        UpdateTime();
    
    return now;
}

void EventLoop::UpdateTime()
{
    now = ::Now();
}

void EventLoop::Stop()
{
    running = false;
}
