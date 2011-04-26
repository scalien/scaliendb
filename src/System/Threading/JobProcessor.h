#ifndef JOBPROCESSOR_H
#define JOBPROCESSOR_H

#include "System/Containers/InQueue.h"
#include "ThreadPool.h"
#include "Job.h"

/*
===============================================================================================

 JobProcessor

===============================================================================================
*/

class JobProcessor
{
public:
    JobProcessor();
    
    void                    Start(unsigned numThreads = 1);
    void                    Execute(Job* job);
    void                    ThreadFunc();
    void                    OnComplete();
    
    bool                    active;
    InQueue<Job>            jobs;
    ThreadPool*             threadPool;
    Callable                threadFunc;
    Callable                onComplete;
};

#endif
