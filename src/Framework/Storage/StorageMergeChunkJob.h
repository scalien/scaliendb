#ifndef STORAGEMERGECHUNKJOB_H
#define STORAGEMERGECHUNKJOB_H

#include "System/Threading/Job.h"
#include "System/Containers/List.h"
#include "System/Buffers/Buffer.h"

class StorageEnvironment;
class StorageFileChunk;

/*
===============================================================================================

 StorageMergeChunkJob

===============================================================================================
*/

class StorageMergeChunkJob : public Job
{
public:
    StorageMergeChunkJob(StorageEnvironment* env,
     uint64_t contextID, uint64_t shardID,
     List<Buffer*>& filenames, StorageFileChunk* mergeChunk,
     ReadBuffer firstKey, ReadBuffer lastKey);
    
    void                Execute();
    void                OnComplete();

    StorageEnvironment* env;
    uint64_t            contextID;
    uint64_t            shardID;
    StorageFileChunk*   mergeChunk;
    List<Buffer*>       filenames;
    Buffer              firstKey;
    Buffer              lastKey;
};

#endif
