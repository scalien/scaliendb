#include "Stopwatch.h"

Stopwatch::Stopwatch()
{
    Reset();
}

void Stopwatch::Reset()
{
    elapsed = 0;
}

void Stopwatch::Restart()
{
    Reset();
    Start();
}

void Stopwatch::Start()
{
    started = Now();
}

uint64_t Stopwatch::Stop()
{
    elapsed += Now() - started;
    return elapsed;
}

uint64_t Stopwatch::Elapsed()
{
    return elapsed;
}
