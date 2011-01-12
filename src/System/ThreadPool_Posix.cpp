#ifndef _WIN32
#include <pthread.h>

#include "ThreadPool.h"
#include "System/Common.h"
#include "System/Log.h"
#include "System/Events/Callable.h"

/*
===============================================================================

 ThreadPool_Pthread

===============================================================================
*/


class ThreadPool_Pthread : public ThreadPool
{
public:
    ThreadPool_Pthread(int numThread);
    ~ThreadPool_Pthread();

    ThreadPool*         Create(int numThread);

    virtual void        Start();
    virtual void        Stop();
    virtual void        WaitStop();

    virtual void        Execute(const Callable& callable);

protected:
    pthread_t*          threads;
    pthread_mutex_t     mutex;
    pthread_cond_t      cond;
    int                 numStarted;
    bool                finished;
    
    static void*        thread_function(void *param);
    void                ThreadFunction();
    bool                IsFinished();
};

/*
===============================================================================
*/

ThreadPool* ThreadPool::Create(int numThread)
{
    return new ThreadPool_Pthread(numThread);
}

uint64_t ThreadPool::GetThreadID()
{
    return (uint64_t) pthread_self();
}

void ThreadPool::Yield()
{
    pthread_yield_np();
}

void* ThreadPool_Pthread::thread_function(void* param)
{
    ThreadPool_Pthread* tp = (ThreadPool_Pthread*) param;
    
    BlockSignals();
    tp->ThreadFunction();
    
    // pthread_exit should be called for cleanup, instead it creates
    // more unreachable bytes with valgrind:
    //pthread_exit(NULL);
    return NULL;
}

void ThreadPool_Pthread::ThreadFunction()
{   
    Callable    callable;
    Callable*   it;
    bool        firstRun;

    firstRun = true;
    
    while (running)
    {
        pthread_mutex_lock(&mutex);
        numActive--;

        if (firstRun)
        {
            numStarted++;
            pthread_cond_broadcast(&cond);
            firstRun = false;
        }
        
    wait:
        while (numPending == 0 && running && !IsFinished())
            pthread_cond_wait(&cond, &mutex);
        
        if (!running || IsFinished())
        {
            pthread_mutex_unlock(&mutex);
            break;
        }
        it = callables.First();
        if (!it)
            goto wait;
            
        callable = *it;
        callables.Remove(it);
        numPending--;
        numActive++;
        
        pthread_mutex_unlock(&mutex);
        
        Call(callable);
    }
    
    if (finished)
    {
        pthread_mutex_lock(&mutex);
        pthread_cond_signal(&cond);
        pthread_mutex_unlock(&mutex);
    }
}

ThreadPool_Pthread::ThreadPool_Pthread(int numThread_)
{
    numThread = numThread_;
    numPending = 0;
    numActive = 0;
    numStarted = 0;
    running = false;
    finished = false;
    
    pthread_mutex_init(&mutex, NULL);
    pthread_cond_init(&cond, NULL);
    
    threads = new pthread_t[numThread];
}

ThreadPool_Pthread::~ThreadPool_Pthread()
{
    Stop();
    delete[] threads;
}

void ThreadPool_Pthread::Start()
{
    int i;
    
    if (running)
        return;
    
    running = true;
    numActive = numThread;
    
    for (i = 0; i < numThread; i++)
        pthread_create(&threads[i], NULL, thread_function, this);
}

void ThreadPool_Pthread::Stop()
{
    int i;
    
    if (!running)
        return;

    running = false;
    
    pthread_mutex_lock(&mutex);
    pthread_cond_broadcast(&cond);
    pthread_mutex_unlock(&mutex);

    for (i = 0; i < numThread; i++)
    {
        pthread_join(threads[i], NULL);
        pthread_detach(threads[i]);
    }
    
    numActive = 0;
}

void ThreadPool_Pthread::WaitStop()
{
    int i;
    
    if (!running)
        return;
    
    pthread_mutex_lock(&mutex);
    while (numStarted < numThread)
        pthread_cond_wait(&cond, &mutex);

    finished = true;

    pthread_cond_broadcast(&cond);
    pthread_mutex_unlock(&mutex);

    for (i = 0; i < numThread; i++)
    {
        pthread_join(threads[i], NULL);
        pthread_detach(threads[i]);
    }
    
    running = false;
    numActive = 0;
}

void ThreadPool_Pthread::Execute(const Callable& callable)
{
    pthread_mutex_lock(&mutex);
    
    callables.Append((Callable&)callable);
    numPending++;
    
    pthread_cond_signal(&cond);
    
    pthread_mutex_unlock(&mutex);
}

bool ThreadPool_Pthread::IsFinished()
{
    return finished && numPending == 0;
}

#endif
