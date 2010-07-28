#include "Timer.h"

Timer::Timer()
{
	when = 0;
	active = false;

	next = this;
	prev = this;
}

void Timer::SetCallable(const Callable& callable_)
{
	callable = callable_;
}
	
void Timer::Set(uint64_t when_)
{
	when = when_;
}

bool Timer::IsActive() const
{
	return active;
}

uint64_t Timer::When() const
{
	return when;
}

void Timer::Execute() const
{
	Call(callable);
}
