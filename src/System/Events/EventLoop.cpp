#include "EventLoop.h"
#include "ArrayList.h"

static volatile uint64_t        now;
static bool                     running;
static ArrayList<Timer*, 1024>  ran;

long EventLoop::RunTimers()
{
    Timer*  timer;
    long    wait;
    
    wait = -1;
    ran.Clear();
    for (timer = timers.First(); timer != NULL; )
    {
        UpdateTime();
        if (timer->GetExpireTime() <= now)
        {
            if (ran.Contains(timer))
            {
                timer = timers.Next(timer);
                wait = 0;
                continue;
            }
            Remove(timer);
            timer->Execute();
            ran.Append(timer);
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

void EventLoop::Stop()
{
    running = false;
}
