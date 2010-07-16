#ifndef TIMER_H
#define TIMER_H

#include "System/Time.h"
#include "Callable.h"

/*
===============================================================================

 Timer

===============================================================================
*/

class Timer
{
	friend class Scheduler;

public:
	Timer();
	virtual ~Timer() {}
		
	void			SetCallable(const Callable& callable);
	void			Set(uint64_t when);

	uint64_t		When() const;	
	bool			IsActive() const;
	
	void			Execute() const;
	
	virtual void	OnAdd() {};
	
	Timer*			next;
	Timer*			prev;

protected:
    bool			active;
	uint64_t		when;
    Callable		callable;
};

/*****************************************************************************/

inline bool LessThan(const Timer& a, const Timer& b)
{
    return (a.When() < b.When());
}

#endif
