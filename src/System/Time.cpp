#include "Time.h"
#include "Threading/ThreadPool.h"
#include "Threading/Atomic.h"
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
 // this is used to make sure clock is always monotonically increasing
static volatile uint64_t    clockCorrectionMsec = 0;
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

#endif // _WIN32

// Returns strictly increasing values when queried from the same thread.
// If you want to have strictly increasing time within different threads,
// then use NowClock()
//
// Do NOT call Log functions from here, because logging may use 
// timestamps by calling Now() and then it would result in an infinite
// loop.
uint64_t Now()
{
    static THREAD_LOCAL uint64_t    prevNow;
    uint64_t                        now;
    struct timeval                  tv;
    
    gettimeofday(&tv, NULL);
    
    now = tv.tv_sec;
    now *= 1000;
    now += tv.tv_usec / 1000;
    now += clockCorrectionMsec;

    // system clock went backwards
    if (prevNow > now)
    {
        // Handling an edge case when Clock-thread is not running or
        // system clock was changed since last UpdateTime and clock
        // was not corrected since then.
        
        // Make time strictly increasing
        now = prevNow + CLOCK_RESOLUTION;
    }

    prevNow = now;

    return now;
}

uint64_t NowClock()
{
    if (!clockStarted)
        return Now();
        
    return clockMsec;
}

static inline void UpdateClock()
{
    static struct timeval tv;
    uint64_t	prevMsec;
    uint64_t    msec;
    uint64_t    diff;

    prevMsec = clockMsec;

    gettimeofday(&tv, NULL);
    msec = (uint64_t)tv.tv_sec * 1000 + tv.tv_usec / 1000 + clockCorrectionMsec;

    if (prevMsec != 0)
    {
        if (prevMsec > msec)
        {
            // system clock went backwards
            diff = prevMsec - msec;
            Log_Debug("Softclock diff negative, possible system clock adjustment: %U", diff);

            // update correction with diff, plus some more to make time strictly increasing
            AtomicExchangeU64(clockCorrectionMsec, clockCorrectionMsec + diff + CLOCK_RESOLUTION);
            
            // recalculate msec
            msec = (uint64_t)tv.tv_sec * 1000 + tv.tv_usec / 1000 + clockCorrectionMsec;
        }
        else if (msec - prevMsec > MAX(2 * CLOCK_RESOLUTION, 100))
        {
            // only logging
            Log_Debug("Softclock diff: %U", msec - prevMsec);
        }
    }

    AtomicExchangeU64(clockMsec, msec);
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
    clockThread->Execute(CFunc(ClockThread));
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
