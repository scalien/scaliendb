#include "Countdown.h"

Countdown::Countdown()
{
    delay = 0;
}

void Countdown::SetDelay(uint64_t delay_)
{
    assert(next == prev && next == this);
    delay = delay_;
}

uint64_t Countdown::GetDelay() const
{
    return delay;
}

void Countdown::OnAdd()
{
    expireTime = Now() + delay;
}
