#include "Timer.h"

Timer::Timer(Callable* callable_)
{
	when = 0;
	callable = callable_;
	active = false;
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
