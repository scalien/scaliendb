#ifndef MUTEX_H
#define MUTEX_H

#include "System/Platform.h"

#ifdef PLATFORM_WINDOWS

/*
===============================================================================================

 mutex_t: platform-independent mutual exclusion helper type

===============================================================================================
*/

#ifdef _WIN64
#define CRITICAL_SECTION_BUFFER_SIZE    40
#else
#define CRITICAL_SECTION_BUFFER_SIZE    24
#endif

union WindowsCriticalSectionUnion
{
    long    padding;
    char    buf[CRITICAL_SECTION_BUFFER_SIZE];
};

typedef WindowsCriticalSectionUnion     mutex_t;

#else // on other platforms than Windows we use Posix Threads

#include <pthread.h>
typedef pthread_mutex_t                 mutex_t;

#endif

/*
===============================================================================================

 Mutex: platform-independent mutual exclusion support class

===============================================================================================
*/

class Mutex
{
public:
    Mutex();
    ~Mutex();
    
    void        Lock();
    bool        TryLock();
    void        Unlock();
    
private:
    mutex_t     mutex;
};

/*
===============================================================================================

 MutexGuarded: scope guarded mutex

===============================================================================================
*/

class MutexGuard
{
public:
    MutexGuard(Mutex& mutex_);
    ~MutexGuard();
    
    void        Lock();
    bool        TryLock();
    void        Unlock();

private:
    Mutex&      mutex;
    bool        locked;
};

inline MutexGuard::MutexGuard(Mutex& mutex_) : mutex(mutex_) 
{ 
    mutex.Lock(); 
    locked = true;
}

inline MutexGuard::~MutexGuard()
{
    Unlock();
}

inline void MutexGuard::Lock()
{
    if (!locked) 
    { 
        mutex.Lock();
        locked = true; 
    }
}

inline void MutexGuard::Unlock()
{
    if (locked)
    {
        mutex.Unlock();
        locked = false;
    }
}

#endif
