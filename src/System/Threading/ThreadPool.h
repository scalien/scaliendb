#ifndef THREADPOOL_H
#define THREADPOOL_H

#include "System/Platform.h"
#include "System/Containers/List.h"
#include "System/Events/Callable.h"

/*
===============================================================================================

 ThreadPool

===============================================================================================
*/

class ThreadPool
{
public:
    static ThreadPool* Create(int numThreads);

    virtual ~ThreadPool() {}

    virtual void        Start() = 0;
    virtual void        Stop() = 0;
    virtual void        WaitStop() = 0;

    virtual void        Execute(const Callable &callable) = 0;
    
    unsigned            GetNumThreads();
    unsigned            GetNumPending();
    unsigned            GetNumActive();
    unsigned            GetNumTotal();
    void                SetStackSize(unsigned stackSize);
    
    static uint64_t     GetThreadID();
    static void         YieldThread();
    
protected:
    List<Callable>      callables;
    unsigned            numPending;
    unsigned            numActive;
    unsigned            numThreads;
    bool                running;
    unsigned            stackSize;
};

/*
===============================================================================
*/

inline unsigned ThreadPool::GetNumThreads()
{
    return numThreads;
}

inline unsigned ThreadPool::GetNumPending()
{
    return numPending;
}

inline unsigned ThreadPool::GetNumActive()
{
    return numActive;
}

inline unsigned ThreadPool::GetNumTotal()
{
    return numPending + numActive; /* atomicity */
}

inline void ThreadPool::SetStackSize(unsigned stackSize_)
{
    stackSize = stackSize_;
}

#endif
