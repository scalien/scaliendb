#include "Scheduler.h"

InSortedList<Timer> Scheduler::timers;

void Scheduler::Add(Timer* timer)
{
    assert(timer->next == timer->prev && timer->next == timer);
    timer->OnAdd();
    timer->active = true;
    timers.Add(timer);
}

void Scheduler::Remove(Timer* timer)
{
    assert(timer->active == (timer->next != timer));
    if (timer->active)
        timers.Remove(timer);
    timer->active = false;
}

void Scheduler::Reset(Timer* timer)
{
    Remove(timer);
    Add(timer);
}

void Scheduler::Shutdown()
{
    while (timers.GetLength() > 0)
        Remove(timers.First());
}
