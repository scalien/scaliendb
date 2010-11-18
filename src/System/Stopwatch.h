#ifndef STOPWATCH_H
#define STOPWATCH_H

#include "Time.h"

/*
===============================================================================================

 Stopwatch

===============================================================================================
*/

class Stopwatch
{
public:
    Stopwatch();
    
    void        Reset();
    void        Restart();
    void        Start();
    uint64_t    Stop();
    uint64_t    Elapsed();

private:
    uint64_t    started;
    uint64_t    elapsed;
};

#endif
