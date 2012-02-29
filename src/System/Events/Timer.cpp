#include "Timer.h"

// This function is copy-pasted from IOProcessor_Windows
static void CallWarnTimeout(Callable& callable)
{
    uint64_t    start;
    uint64_t    elapsed;

    start = NowClock();
    Call(callable);
    elapsed = NowClock() - start;
    if (elapsed > 100)
        Log_Debug("EventLoop callback elapsed time: %U", elapsed);
}

Timer::Timer()
{
    expireTime = 0;
    active = false;
    ran = false;

    next = this;
    prev = this;
}

Timer::~Timer()
{
    ASSERT(active == false);
    ASSERT(next == prev && next == this);
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
    CallWarnTimeout(callable);
}

YieldTimer::YieldTimer()
{
    expireTime = 0;
}

void YieldTimer::SetExpireTime(uint64_t)
{
    STOP_FAIL(1, "Program bug: YieldTimer::SetExpireTime() called");
}
