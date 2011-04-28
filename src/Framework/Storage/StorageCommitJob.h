#ifndef STORAGECOMMITJOB_H
#define STORAGECOMMITJOB_H

#include "System/Threading/Job.h"
#include "StorageLogSegment.h"

class StorageEnvironment;

/*
===============================================================================================

 StorageCommitJob

===============================================================================================
*/

class StorageCommitJob : public Job
{
public:
    StorageCommitJob(StorageEnvironment* env_, StorageLogSegment* logSegment);
    
    void                Execute();
    void                OnComplete();
    
    StorageEnvironment* env;
    StorageLogSegment*  logSegment;
};

#endif
