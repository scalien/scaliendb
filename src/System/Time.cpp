#include "Time.h"
#include "ThreadPool.h"
#include "Events/Callable.h"

#ifdef _WIN32
#include <time.h>
#include <windows.h>
#else
#include <sys/time.h>
#endif

// independent thread clock support
static int                  clockStarted = 0;
static volatile uint64_t    clockMsec = 0;
static volatile uint64_t    clockUsec = 0;
static ThreadPool*          clockThread = NULL;

#ifdef _WIN32
int gettimeofday (struct timeval *tv, void* tz)
{
    union
    {
        LONGLONG ns100; // time since 1 Jan 1601 in 100ns units
        FILETIME ft;
    } now;

    GetSystemTimeAsFileTime (&now.ft);
    tv->tv_usec = (long) ((now.ns100 / 10LL) % 1000000LL);
    tv->tv_sec = (long) ((now.ns100 - 116444736000000000LL) / 10000000LL);
    return (0);
}
#endif

uint64_t Now()
{
    uint64_t now;
    struct timeval tv;
    
    gettimeofday(&tv, NULL);
    
    now = tv.tv_sec;
    now *= 1000;
    now += tv.tv_usec / 1000;

    return now;
}

uint64_t NowMicro()
{
    uint64_t now;
    struct timeval tv;
    
    gettimeofday(&tv, NULL);
    
    now = tv.tv_sec;
    now *= 1000000;
    now += tv.tv_usec;
    
    return now;
}

uint64_t NowClock()
{
    if (!clockStarted)
        return Now();
        
    return clockMsec;
}

uint64_t NowMicroClock()
{
    if (!clockStarted)
        return NowMicro();
    
    return clockUsec;
}

static inline void UpdateClock()
{
    static struct timeval tv;

    gettimeofday(&tv, NULL);
    clockMsec = (uint64_t)tv.tv_sec * 1000 + tv.tv_usec / 1000;
    clockUsec = (uint64_t)tv.tv_sec * 1000000 + tv.tv_usec;
}

static void ClockThread()
{
    while (clockStarted)
    {
        UpdateClock();
        MSleep(20);
    }
}

void StartClock()
{
    if (clockThread)
        return;
    
    UpdateClock();

    clockStarted = 1;
    clockThread = ThreadPool::Create(1);
    clockThread->Start();
    clockThread->Execute(Callable(CFunc(ClockThread)));
}

void StopClock()
{
    if (!clockThread)
        return;
        
    clockStarted = 0;
    clockThread->Stop();
    delete clockThread;
    clockThread = NULL;
}

#ifndef _WIN32
void MSleep(unsigned long msec)
{
    usleep(msec * 1000);
}
#else
void MSleep(unsigned long msec)
{
    Sleep(msec);
}
#endif
