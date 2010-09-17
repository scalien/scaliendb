#include "Timer.h"

Timer::Timer()
{
	expireTime = 0;
	active = false;

	next = this;
	prev = this;
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
