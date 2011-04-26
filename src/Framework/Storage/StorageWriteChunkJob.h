#ifndef STORAGEWRITECHUNKJOB_H
#define STORAGEWRITECHUNKJOB_H

#include "System/Threading/Job.h"

class StorageEnvironment;
class StorageFileChunk;

/*
===============================================================================================

 StorageWriteChunkJob

===============================================================================================
*/

class StorageWriteChunkJob : public Job
{
public:
    StorageWriteChunkJob(StorageEnvironment* env, StorageFileChunk* chunk);
    
    void                Execute();
    void                OnComplete();
    
private:
    StorageEnvironment* env;
    StorageFileChunk*   fileChunk;
};

#endif
