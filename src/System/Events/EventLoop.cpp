#include "EventLoop.h"
#include "System/IO/IOProcessor.h"

static volatile uint64_t        now;
static bool                     running;
static bool                     started;

long EventLoop::RunTimers()
{
    Timer*      timer;
    long        wait;
    uint64_t    prev;
    
    wait = -1;
    prev = 0;

#ifdef EVENTLOOP_MULTITHREADED
    MutexGuard guard(mutex);
#endif

    FOREACH (timer, timers)
        timer->ran = false;

    for (timer = timers.First(); timer != NULL; )
    {
        UpdateTime();
        if (prev != 0 && now - prev > 100)
            Log_Debug("EventLoop callback elapsed time: %U", now - prev);
        prev = now;

        if (timer->GetExpireTime() <= now)
        {
            if (timer->ran)
            {
                timer = timers.Next(timer);
                wait = 0;
                continue;
            }
            UnprotectedRemove(timer);
            timer->ran = true;
            
#ifdef EVENTLOOP_MULTITHREADED
            mutex.Unlock();
#endif
            timer->Execute();
#ifdef EVENTLOOP_MULTITHREADED
            mutex.Lock();
#endif

            timer = timers.First();
        }
        else
            return (wait == 0 ? 0 : timer->GetExpireTime() - now);
    }

    return wait;
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
    running = false;
}

void EventLoop::Init()
{
    running = false;
    started = false;
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

void EventLoop::Start()
{
    running = true;
    started = true;
}

void EventLoop::Stop()
{
    running = false;
}

bool EventLoop::IsRunning()
{
    return running;
}

bool EventLoop::IsStarted()
{
    return started;
}
