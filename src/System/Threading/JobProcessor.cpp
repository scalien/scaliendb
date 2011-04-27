#include "JobProcessor.h"
#include "System/IO/IOProcessor.h"

JobProcessor::JobProcessor()
{
    threadFunc = MFUNC(JobProcessor, ThreadFunc);
    onComplete = MFUNC(JobProcessor, OnComplete);
}

void JobProcessor::Start()
{
    threadPool = ThreadPool::Create(1);
    threadPool->Start();
    active = false;
}

void JobProcessor::Stop()
{
    threadPool->Stop();
}

void JobProcessor::Execute(Job* job)
{
    // (runs in main thread)
    // add the job to the threadpool
    // the job will execute ThreadFunc() in this class
    // increase numJobs

    jobs.Enqueue(job);
    if (!active)
    {
        active = true;
        threadPool->Execute(threadFunc);
    }
}

bool JobProcessor::IsActive()
{
    return (jobs.GetLength() > 0);
}

void JobProcessor::ThreadFunc()
{
    // (runs in another thread)
    // call the StorageJob's Execute()
    // then IOProcessor::Complete()
    // with OnComplete() in this class

    ASSERT(jobs.GetLength() > 0);
    jobs.First()->Execute();
    IOProcessor::Complete(&onComplete);
}

void JobProcessor::OnComplete()
{
    // (runs in main thread)
    // called when the job is finished
    // decrease numJobs
    // call the job's OnComplete()
    // then we're done

    ASSERT(jobs.GetLength() > 0);
    ASSERT(active);
    jobs.Dequeue()->OnComplete();
    
    if (jobs.GetLength() > 0)
        threadPool->Execute(threadFunc);
    else
        active = false;
}
