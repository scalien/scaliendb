#ifdef PLATFORM_WINDOWS
#include "Mutex.h"
#include "System/Macros.h"

#include <windows.h>

// This is here to ensure that the mutex type has enough space to 
// hold a Windows CRITICAL_SECTION object
STATIC_ASSERT(sizeof(CRITICAL_SECTION) == CRITICAL_SECTION_BUFFER_SIZE, 
    "CRITICAL_SECTION_BUFFER_SIZE must be equal to sizeof(CRITICAL_SECTION)");

Mutex::Mutex()
{
    InitializeCriticalSection((CRITICAL_SECTION*) &mutex);
    name = "";
}

Mutex::~Mutex()
{
    DeleteCriticalSection((CRITICAL_SECTION*) &mutex);
}

void Mutex::Lock()
{
    EnterCriticalSection((CRITICAL_SECTION*) &mutex);
    threadID = (uint64_t) GetCurrentThreadId();
}

void Mutex::Unlock()
{
    threadID = 0;
    LeaveCriticalSection((CRITICAL_SECTION*) &mutex);
}

#endif
