#ifdef _WIN32
#define _WIN32_WINNT            0x0400  // Windows NT 4.0
#define WIN32_LEAN_AND_MEAN

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <windows.h>
#include <process.h>
#include "ThreadPool.h"
#include "System/Events/Callable.h"

/*
===============================================================================

 ThreadPool_Windows

===============================================================================
*/

class ThreadPool_Windows : public ThreadPool
{
public:
    ThreadPool_Windows(int numThread);
    ~ThreadPool_Windows();

    void                        Shutdown();

    virtual void                Start();
    virtual void                Stop();
    virtual void                WaitStop();
    virtual void                Execute(const Callable& callable);
    
private:
    HANDLE*                     threads;
    CRITICAL_SECTION            critsec;
    HANDLE                      startEvent;
    HANDLE                      messageEvent;
    HANDLE                      stopEvent;
    unsigned                    numStarted;
    unsigned                    numStopped;
    bool                        finished;

    static DWORD __stdcall      ThreadFunc(void* arg);
    void                        ThreadPoolFunc();
    bool                        IsFinished();
};

/*
===============================================================================
*/

ThreadPool* ThreadPool::Create(int numThread)
{
    return new ThreadPool_Windows(numThread);
}

uint64_t ThreadPool::GetThreadID()
{
    return GetCurrentThreadId();
}

void ThreadPool::YieldThread()
{
    SwitchToThread();
}

ThreadPool_Windows::ThreadPool_Windows(int numThreads_)
{
    InitializeCriticalSection(&critsec);
    numThreads = numThreads_;
    numPending = 0;
    numStarted = 0;
    numStopped = 0;
    running = false;
    finished = false;
    threads = NULL;
    stackSize = 0;

    // create the event as an auto-reset event object, set to nonsignaled state
    startEvent = CreateEvent(NULL, FALSE, FALSE, NULL);
    messageEvent = CreateEvent(NULL, FALSE, FALSE, NULL);
    stopEvent = CreateEvent(NULL, FALSE, FALSE, NULL);
}

ThreadPool_Windows::~ThreadPool_Windows()
{
    Stop();
}

void ThreadPool_Windows::Shutdown()
{
    unsigned i;

    if (threads)
    {
        // the finished thread will call SetEvent again, see ThreadPoolFunc
        SetEvent(messageEvent);

        // XXX: this is a workaround as WaitForMultipleObjects cannot handle more than 64 handles
        // wait for all thread is stopped
        EnterCriticalSection(&critsec);
        while (numStopped < numThreads)
        {
            LeaveCriticalSection(&critsec);
            WaitForSingleObject(stopEvent, INFINITE);
            EnterCriticalSection(&critsec);
        }
        LeaveCriticalSection(&critsec);

        for (i = 0; i < numThreads; i++)
            CloseHandle(threads[i]);
        
        delete[] threads;
        threads = NULL;
        Log_Debug("All threads are stopped, %p", this);
    }

    if (messageEvent)
    {
        CloseHandle(messageEvent);
        messageEvent = NULL;
    }
    if (startEvent)
    {
        CloseHandle(startEvent);
        startEvent = NULL;
    }
    if (stopEvent)
    {
        CloseHandle(stopEvent);
        stopEvent = NULL;
    }

    DeleteCriticalSection(&critsec);
}

void ThreadPool_Windows::Start()
{
    unsigned    i;
    HANDLE      ret;

    if (running)
        return;

    running = true;
    numActive = numThreads;

    threads = new HANDLE[numThreads];
    for (i = 0; i < numThreads; i++)
    {
        ret = CreateThread(NULL, stackSize, ThreadFunc, this, STACK_SIZE_PARAM_IS_A_RESERVATION, NULL);
        if (ret == 0)
        {
            Log_SetTrace(true);
            Log_Errno("Cannot create thread! (errno = %d)", errno);
            numThreads = i;
            break;
        }

        threads[i] = (HANDLE)ret;
    }
}

void ThreadPool_Windows::Stop()
{
    if (!running)
        return;

    running = false;
    Shutdown();
}

void ThreadPool_Windows::WaitStop()
{
    //TODO: write this function properly
    if (!running)
        return;

    // wait for all thread is started
    EnterCriticalSection(&critsec);
    while (numStarted < numThreads)
    {
        LeaveCriticalSection(&critsec);
        WaitForSingleObject(startEvent, INFINITE);
        EnterCriticalSection(&critsec);
    }

    Log_Debug("All threads started, waiting for threads to stop, numThread: %u, %p", numThreads, this);

    finished = true;
    LeaveCriticalSection(&critsec);

    Shutdown();
    running = false;
}

void ThreadPool_Windows::Execute(const Callable& callable)
{
    EnterCriticalSection(&critsec);

    callables.Append((Callable&)callable);
    numPending++;

    LeaveCriticalSection(&critsec);
    SetEvent(messageEvent);
}

void ThreadPool_Windows::ThreadPoolFunc()
{
    Callable    callable;
    Callable*   it;
    bool        notifyNext;

    SeedRandomWith(ThreadPool::GetThreadID());

    if (running)
    {
        EnterCriticalSection(&critsec);
        numStarted++;
        LeaveCriticalSection(&critsec);
        SetEvent(startEvent);
    }

    while (running)
    {
        // TODO: simplify the logic here & make it similar to Posix implementation
        if (running)
            WaitForSingleObject(messageEvent, INFINITE);

        while (true)
        {
            EnterCriticalSection(&critsec);

            numActive--;

            if (!running || IsFinished())
            {
                LeaveCriticalSection(&critsec);
                goto End;
            }
            
            it = callables.First();
            if (it)
            {
                callable = *it;
                callables.Remove(it);
                numPending--;
            }
            else
            {
                numActive++;
                LeaveCriticalSection(&critsec);
                break;
            }

            notifyNext = false;
            if (numPending > 0)
                notifyNext = true;
            numActive++;
            LeaveCriticalSection(&critsec);

            if (notifyNext)
                SetEvent(messageEvent);

            Call(callable);
        }
    }

End:
    // wake up next sleeping thread
    SetEvent(messageEvent);

    EnterCriticalSection(&critsec);
    numStopped++;
    LeaveCriticalSection(&critsec);
    SetEvent(stopEvent);
}

DWORD ThreadPool_Windows::ThreadFunc(void *arg)
{
    ThreadPool_Windows *tp = (ThreadPool_Windows *) arg;
    tp->ThreadPoolFunc();

    return 0;
}

bool ThreadPool_Windows::IsFinished()
{
    return finished && numPending == 0;
}

#endif
