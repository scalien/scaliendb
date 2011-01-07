#include "Timer.h"

Timer::Timer()
{
    expireTime = 0;
    active = false;

    next = this;
    prev = this;
}

Timer::~Timer()
{
//    assert(active == false);
//    assert(next == prev && next == this);
}

void Timer::SetCallable(Callable callable_)
{
    callable = callable_;
}
    
void Timer::SetExpireTime(uint64_t expireTime_)
{
    expireTime = expireTime_;
}

bool Timer::IsActive()
{
    return active;
}

uint64_t Timer::GetExpireTime()
{
    return expireTime;
}

void Timer::Execute()
{
    Call(callable);
}

YieldTimer::YieldTimer()
{
    expireTime = 0;
}

void YieldTimer::SetExpireTime(uint64_t)
{
    STOP_FAIL(1, "Program bug: YieldTimer::SetExpireTime() called");
}
