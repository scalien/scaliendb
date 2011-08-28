#include "EventLoop.h"
#include "System/Containers/ArrayList.h"

static volatile uint64_t        now;
static bool                     running;

long EventLoop::RunTimers()
{
    Timer*  timer;
    long    wait;
    
    wait = -1;

#ifdef EVENTLOOP_MULTITHREADED
    MutexGuard guard(mutex);
#endif

    FOREACH (timer, timers)
        timer->ran = false;

    for (timer = timers.First(); timer != NULL; )
    {
        UpdateTime();
        if (timer->GetExpireTime() <= now)
        {
            if (timer->ran)
            {
                timer = timers.Next(timer);
                wait = 0;
                continue;
            }
            UnprotectedRemove(timer);

#ifdef EVENTLOOP_MULTITHREADED            
            mutex.Unlock();
            IOProcessor::Lock();
#endif
            timer->Execute();

#ifdef EVENTLOOP_MULTITHREADED
            IOProcessor::Unlock();
            mutex.Lock();
#endif            
            timer->ran = true;
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

void EventLoop::Start()
{
    running = true;
}

void EventLoop::Stop()
{
    running = false;
}

bool EventLoop::IsRunning()
{
    return running;
}
