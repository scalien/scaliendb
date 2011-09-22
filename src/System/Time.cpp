#include "Time.h"
#include "Threading/ThreadPool.h"
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
static int                  winTimeInitialized = 0;
static uint64_t             winBaseMillisec = 0;
static volatile uint64_t	winElapsed = 0;
static volatile uint32_t    winPrevMillisec = 0;
static CRITICAL_SECTION		timerCritSec;

int gettimeofday_win(struct timeval* tv, void*)
{
    union
    {
        LONGLONG ns100; // time since 1 Jan 1601 in 100ns units
        FILETIME ft;
    } now;

    // this has 15 msec resolution
    GetSystemTimeAsFileTime(&now.ft);
    
    // convert it to Unix epoch
    tv->tv_usec = (long) ((now.ns100 / 10LL) % 1000000LL);
    tv->tv_sec = (long) ((now.ns100 - 116444736000000000LL) / 10000000LL);

    return 0;
}

void InitializeWindowsTimer()
{
    struct timeval  tv;

    // set the resolution of the multimedia-timer to 1 msec
    timeBeginPeriod(1);
    
    // returns the number of milliseconds since system start
    winPrevMillisec = timeGetTime();

    gettimeofday_win(&tv, NULL);
    
    // convert system start time to Unix timestamp
    winBaseMillisec = (tv.tv_sec * 1000LL + tv.tv_usec / 1000) - winPrevMillisec;
    
	InitializeCriticalSection(&timerCritSec);

    winTimeInitialized = 1;
}

int gettimeofday(struct timeval *tv, void*)
{
    uint64_t    elapsedTime;
    uint32_t    winMsec;
	uint64_t	prevMsec;
    uint64_t    msec;
    
    // by default gettimeofday uses the standard Windows clock, which has 15 msec resolution
    // if more resolution needed, comment the following line
    return gettimeofday_win(tv, NULL);

    if (!winTimeInitialized)
        InitializeWindowsTimer();

	// avoid race conditions
	prevMsec = winPrevMillisec;
    winMsec = timeGetTime();

	// handle overflow
    if (winMsec < prevMsec)
	{
		Log_Trace("overflow");
        elapsedTime = (uint64_t)(0x100000000LL - prevMsec) + winMsec;
	}
    else
        elapsedTime = winMsec - prevMsec;

    winPrevMillisec = winMsec;
	winElapsed += elapsedTime;

    msec = winBaseMillisec + winElapsed;
    tv->tv_usec = (msec % 1000) * 1000;
    tv->tv_sec = msec / 1000;

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
    uint64_t	prevMsec;

    prevMsec = clockMsec;

    gettimeofday(&tv, NULL);
    clockMsec = (uint64_t)tv.tv_sec * 1000 + tv.tv_usec / 1000;
    clockUsec = (uint64_t)tv.tv_sec * 1000000 + tv.tv_usec;

    if (prevMsec != 0 && clockMsec - prevMsec > 5 * CLOCK_RESOLUTION)
        Log_Debug("Softclock diff: %U", clockMsec - prevMsec);
}

static void ClockThread()
{
    while (clockStarted)
    {
        UpdateClock();
        MSleep(CLOCK_RESOLUTION);
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
