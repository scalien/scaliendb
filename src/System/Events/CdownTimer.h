#ifndef CDOWNTIMER_H
#define CDOWNTIMER_H

#include "Timer.h"

/*
 * CdownTimer
 */

class CdownTimer : public Timer
{
public:
	CdownTimer(Callable* callable_);
	CdownTimer(uint64_t delay_, Callable* callable_);
	
	virtual	~CdownTimer() {};
		
	void			SetDelay(uint64_t delay_);
	uint64_t		GetDelay() const;

	virtual void	OnAdd();
	
private:
	uint64_t		delay;
};

#endif
