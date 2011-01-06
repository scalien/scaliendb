#ifndef THREADPOOL_H
#define THREADPOOL_H

#include "System/Containers/List.h"

class Callable; // forward

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
    
protected:
    List<Callable>      callables;
    int                 numPending;
    int                 numActive;
    int                 numThread;
    bool                running;
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


#endif
