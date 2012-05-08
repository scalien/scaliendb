#ifdef PLATFORM_WINDOWS
#include "Mutex.h"
#include "System/Macros.h"
#include "System/Time.h"

#include <windows.h>

// This is here to ensure that the mutex type has enough space to 
// hold a Windows CRITICAL_SECTION object
STATIC_ASSERT(sizeof(CRITICAL_SECTION) == CRITICAL_SECTION_BUFFER_SIZE, 
    "CRITICAL_SECTION_BUFFER_SIZE must be equal to sizeof(CRITICAL_SECTION)");

Mutex::Mutex()
{
    InitializeCriticalSection((CRITICAL_SECTION*) &mutex);
    name = "";
    lockCounter = 0;
    lastLockTime = 0;
}

Mutex::~Mutex()
{
    DeleteCriticalSection((CRITICAL_SECTION*) &mutex);
}

void Mutex::Lock()
{
    EnterCriticalSection((CRITICAL_SECTION*) &mutex);
    threadID = (uint64_t) GetCurrentThreadId();
    lockCounter++;
    lastLockTime = NowClock();
}

bool Mutex::TryLock()
{
    BOOL    ret;
    
    ret = TryEnterCriticalSection((CRITICAL_SECTION*) &mutex);
    if (ret)
        threadID = (uint64_t) GetCurrentThreadId();
    
    return ret ? true : false;
} 

void Mutex::Unlock()
{
    threadID = 0;
    LeaveCriticalSection((CRITICAL_SECTION*) &mutex);
}

#endif
