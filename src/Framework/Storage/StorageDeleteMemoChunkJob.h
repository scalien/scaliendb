#ifndef STORAGEDELETEMEMOCHUNKJOB_H
#define STORAGEDELETEMEMOCHUNKJOB_H

#include "System/Threading/Job.h"

class StorageMemoChunk;

/*
===============================================================================================

 StorageDeleteMemoChunkJob

===============================================================================================
*/

class StorageDeleteMemoChunkJob : public Job
{
public:
    StorageDeleteMemoChunkJob(StorageMemoChunk* chunk);
    
    void                Execute();
    void                OnComplete();

    StorageMemoChunk*   chunk;
};

#endif
