#ifndef TIME_H
#define TIME_H

#include "Platform.h"

#define CLOCK_RESOLUTION    5

/*
===============================================================================================

 Time: platform-independent time interface

===============================================================================================
*/

uint64_t    Now();
uint64_t    NowClock();

void        StartClock();
void        StopClock();

void        MSleep(unsigned long msec);

#endif
