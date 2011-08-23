#include "Scheduler.h"
#include "System/Macros.h"

InSortedList<Timer> Scheduler::timers;
#ifdef EVENTLOOP_MULTITHREADED
Mutex Scheduler::mutex;
#endif

void Scheduler::Add(Timer* timer)
{
#ifdef EVENTLOOP_MULTITHREADED
    MutexGuard guard(mutex);
#endif
    UnprotectedAdd(timer);
}

void Scheduler::Remove(Timer* timer)
{
#ifdef EVENTLOOP_MULTITHREADED
    MutexGuard guard(mutex);
#endif
    UnprotectedRemove(timer);
}

void Scheduler::Reset(Timer* timer)
{
#ifdef EVENTLOOP_MULTITHREADED
    MutexGuard guard(mutex);
#endif
    UnprotectedRemove(timer);
    UnprotectedAdd(timer);
}

void Scheduler::Shutdown()
{
#ifdef EVENTLOOP_MULTITHREADED
    MutexGuard guard(mutex);
#endif
    while (timers.GetLength() > 0)
        UnprotectedRemove(timers.First());
}

unsigned Scheduler::GetNumTimers()
{
    return timers.GetLength();
}

void Scheduler::UnprotectedAdd(Timer* timer)
{
    ASSERT(timer->next == timer->prev && timer->next == timer);
    timer->OnAdd();
    timer->active = true;
    timers.Add(timer);
}

void Scheduler::UnprotectedRemove(Timer* timer)
{
    ASSERT(timer->active == (timer->next != timer));
    if (timer->active)
        timers.Remove(timer);
    timer->active = false;
}
