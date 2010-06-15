#ifndef TIMER_H
#define TIMER_H

#include "System/Time.h"
#include "Callable.h"

/*
 * Timer
 */

class Timer
{
friend class Scheduler;

public:
	Timer();
	virtual ~Timer() {}
		
	void			SetCallable(const Callable& callable_);
	void			Set(uint64_t when_);

	uint64_t		When() const;	
	bool			IsActive() const;
	
	void			Execute() const;
	
	virtual void	OnAdd() {};

protected:
    bool			active;
	uint64_t		when;
    Callable		callable;
};

/*****************************************************************************/

inline bool LessThan(Timer* a, Timer* b)
{
    return (a->When() < b->When());
}

#endif
