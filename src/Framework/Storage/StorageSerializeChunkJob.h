#ifndef STORAGESERIALIZECHUNKJOB_H
#define STORAGESERIALIZECHUNKJOB_H

#include "System/Threading/Job.h"

class StorageEnvironment;
class StorageMemoChunk;

/*
===============================================================================================

 StorageSerializeChunkJob

===============================================================================================
*/

class StorageSerializeChunkJob : public Job
{
public:
    StorageSerializeChunkJob(StorageEnvironment* env, StorageMemoChunk* chunk);
    
    void                Execute();
    void                OnComplete();
    
    StorageEnvironment* env;
    StorageMemoChunk*   memoChunk;
};

#endif
