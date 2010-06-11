#ifndef THREADPOOL_H
#define THREADPOOL_H

#include "System/Containers/List.h"

class Callable;

class ThreadPool
{
public:
	static ThreadPool* Create(int numThread);

	virtual ~ThreadPool() {}

	virtual void	Start() = 0;
	virtual void	Stop() = 0;

	virtual void	Execute(Callable *callable) = 0;
	
	int	NumPending()	{ return numPending; }
	int	NumActive()		{ return numActive; }
	int	NumTotal()		{ return numPending + numActive; /* atomicity */ }
	
protected:
	List<Callable*>		callables;
	int					numPending;
	int					numActive;
	int					numThread;
	bool				running;
};


#endif
