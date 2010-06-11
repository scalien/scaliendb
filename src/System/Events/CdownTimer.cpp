#include "CdownTimer.h"

CdownTimer::CdownTimer(Callable* callable_)
: Timer(callable_)
{
	delay = 0;
}

CdownTimer::CdownTimer(uint64_t delay_, Callable* callable_)
: Timer(callable_)
{
	delay = delay_;
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
