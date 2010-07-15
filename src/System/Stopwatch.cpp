#include "Stopwatch.h"

Stopwatch::Stopwatch()
{
	Reset();
}

void Stopwatch::Reset()
{
	elapsed = 0;
}

void Stopwatch::Start()
{
	started = Now();
}

long Stopwatch::Stop()
{
	elapsed += Now() - started;
	return elapsed;
}

long Stopwatch::Elapsed()
{
	return elapsed;
}