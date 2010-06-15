#include "CdownTimer.h"

CdownTimer::CdownTimer()
: Timer()
{
	delay = 0;
}

void CdownTimer::SetCallable(const Callable& callable_)
{
	Timer::SetCallable(callable_);
	delay = 0;
}

void CdownTimer::SetDelay(uint64_t delay_)
{
	delay = delay_;
}

uint64_t CdownTimer::GetDelay() const
{
	return delay;
}

void CdownTimer::OnAdd()
{
	when = Now() + delay;
}
