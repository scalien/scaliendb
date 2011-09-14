#include "Signal.h"
#include "ThreadPool.h"
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
    bool    stuck;

    SetWaiting(true);
    stuck = false;

    while (true)
    {
        DWORD timeout = 10000;
        //ret = WaitForSingleObject((HANDLE) impl.event, INFINITE);
        ret = WaitForSingleObject((HANDLE) impl.event, timeout);
        if (ret == WAIT_FAILED)
            Log_Errno();
        if (ret != WAIT_OBJECT_0 && ret != WAIT_TIMEOUT)
            Log_Debug("WaitForSingleObject: ret %d", ret);

        if (ret == WAIT_TIMEOUT)
        {
            if (stuck == false)
            {
                //Log_Debug("Waiting for long: %p in %U", this, ThreadPool::GetThreadID());
                stuck = true;
            }
            continue;
        }
        break;
    }
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
