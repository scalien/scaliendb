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
    activeJob = NULL;
}

void JobProcessor::Stop()
{    
    threadPool->Stop();
    delete threadPool;
    threadPool = NULL;
    jobs.DeleteQueue();
}

void JobProcessor::Enqueue(Job* job)
{
    jobs.Enqueue(job);
}

void JobProcessor::Execute()
{
    if (!activeJob && jobs.GetLength() > 0)
    {
        activeJob = jobs.First();
        threadPool->Execute(threadFunc);
    }
}

void JobProcessor::Execute(Job* job)
{
    // (runs in main thread)
    // add the job to the queue
    // the job will execute ThreadFunc() in this class

    jobs.Enqueue(job);
    Execute();
}

bool JobProcessor::IsActive()
{
    return (activeJob != NULL);
}

Job* JobProcessor::GetActiveJob()
{
    ASSERT(activeJob != NULL);
    
    return activeJob;
}

Job* JobProcessor::First()
{
    return jobs.First();
}

Job* JobProcessor::Next(Job* job)
{
    return jobs.Next(job);
}

void JobProcessor::ThreadFunc()
{
    // (runs in another thread)
    // call the StorageJob's Execute()
    // then IOProcessor::Complete()
    // with OnComplete() in this class

    ASSERT(jobs.GetLength() > 0);
    activeJob->Execute();
    IOProcessor::Complete(&onComplete);
}

void JobProcessor::OnComplete()
{
    Job* job;
    
    // (runs in main thread)
    // called when the job is finished
    // call the job's OnComplete()
    // start next pending job

    ASSERT(jobs.GetLength() > 0);
    job = jobs.Dequeue();
    ASSERT(job == activeJob);
    activeJob = NULL;
    job->OnComplete();

    Execute();
}
