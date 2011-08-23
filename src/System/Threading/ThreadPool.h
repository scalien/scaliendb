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
    static ThreadPool* Create(int numThread);

    virtual ~ThreadPool() {}

    virtual void        Start() = 0;
    virtual void        Stop() = 0;
    virtual void        WaitStop() = 0;

    virtual void        Execute(const Callable &callable) = 0;
    
    int                 GetNumPending();
    int                 GetNumActive();
    int                 GetNumTotal();
    void                SetStackSize(unsigned stackSize);
    
    static uint64_t     GetThreadID();
    static void         YieldThread();
    
protected:
    List<Callable>      callables;
    int                 numPending;
    int                 numActive;
    int                 numThread;
    bool                running;
    unsigned            stackSize;
};

/*
===============================================================================
*/

inline int ThreadPool::GetNumPending()
{
    return numPending;
}

inline int ThreadPool::GetNumActive()
{
    return numActive;
}

inline int ThreadPool::GetNumTotal()
{
    return numPending + numActive; /* atomicity */
}

inline void ThreadPool::SetStackSize(unsigned stackSize_)
{
    stackSize = stackSize_;
}

#endif
