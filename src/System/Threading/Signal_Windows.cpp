#include "Signal.h"
#include "System/Common.h"
#include "System/Macros.h"

#include <windows.h>

Signal::Signal()
{
    InitializeCriticalSection((CRITICAL_SECTION*) &impl.mutex);

    // create the event as an auto-reset event object, set to nonsignaled state
    impl.event = (intptr_t) CreateEvent(NULL, FALSE, FALSE, NULL);

    signaled = false;
    waiting = false;
}

Signal::~Signal()
{
    CloseHandle((HANDLE) impl.event);
    DeleteCriticalSection((CRITICAL_SECTION*) &impl.mutex);
}

bool Signal::IsWaiting()
{
    return waiting;
}

void Signal::SetWaiting(bool waiting_)
{
    EnterCriticalSection((CRITICAL_SECTION*) &impl.mutex);
    
    waiting = waiting_;
    if (waiting)
        signaled = false;

    LeaveCriticalSection((CRITICAL_SECTION*) &impl.mutex);
}

void Signal::Wake()
{
    EnterCriticalSection((CRITICAL_SECTION*) &impl.mutex);
    
    UnprotectedWake();
    
    LeaveCriticalSection((CRITICAL_SECTION*) &impl.mutex);
}

void Signal::Wait()
{
    DWORD   ret;

    SetWaiting(true);
    ret = WaitForSingleObject((HANDLE) impl.event, INFINITE);
    if (ret == WAIT_FAILED)
        Log_Errno();
    if (ret != WAIT_OBJECT_0)
        Log_Debug("WaitForSingleObject: ret %d", ret);
    SetWaiting(false);
}

void Signal::Lock()
{
    EnterCriticalSection((CRITICAL_SECTION*) &impl.mutex);
}

void Signal::Unlock()
{
    LeaveCriticalSection((CRITICAL_SECTION*) &impl.mutex);
}

void Signal::UnprotectedWake()
{
    if (waiting && !signaled)
    {
        SetEvent((HANDLE) impl.event);
        signaled = true;
    }
}
