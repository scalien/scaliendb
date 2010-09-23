#ifndef TIME_H
#define TIME_H

#include "Platform.h"

uint64_t    Now();
uint64_t    NowMicro();

uint64_t    NowClock();
uint64_t    NowMicroClock();

void        StartClock();
void        StopClock();

void        MSleep(unsigned long msec);

#endif
