#ifndef STOPWATCH_H
#define STOPWATCH_H

#include "Time.h"

class Stopwatch
{
public:
	Stopwatch () { Reset(); }
	
	void Reset()	{ elapsed = 0; }
	void Start()	{ started = Now(); }
	long Stop()		{ elapsed += Now() - started; return elapsed; }
	long Elapsed()	{ return elapsed; }

private:
	long started;
	long elapsed;
};

#endif
