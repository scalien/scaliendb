#ifndef SIGNAL_H
#define SIGNAL_H

#include "System/Platform.h"
#include "System/Threading/Mutex.h"

/*
===============================================================================================

 SignalImpl -- helper struct for platform independence

===============================================================================================
*/

#ifdef PLATFORM_WINDOWS
struct SignalImpl
{
    mutex_t             mutex;  // this comes from Mutex.h, can be used for CRITICAL_SECTION
    intptr_t            event;  // for holding a HANDLE
};

#else // PLATFORM_WINDOWS
#include <pthread.h>

struct SignalImpl
{
    pthread_mutex_t     mutex;
    pthread_cond_t      cond;
};

#endif // PLATFORM_WINDOWS

/*
===============================================================================================

 Signal

===============================================================================================
*/

class Signal
{
public:
    Signal();
    ~Signal();
    
    bool                IsWaiting();
    void                SetWaiting(bool waiting);
    void                Wake();
    void                Wait();
    
private:
    bool                signaled;
    bool                waiting;
    SignalImpl          impl;
};

#endif
