#ifndef STOPWATCH_H
#define STOPWATCH_H

#include "Time.h"

/*
===============================================================================

 Stopwatch

===============================================================================
*/

class Stopwatch
{
public:
    Stopwatch();
    
    void    Reset();
    void    Restart();
    void    Start();
    long    Stop();
    long    Elapsed();

private:
    long    started;
    long    elapsed;
};

#endif
