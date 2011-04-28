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
    StorageWriteChunkJob(StorageEnvironment* env, StorageFileChunk* writeChunk);
    
    void                Execute();
    void                OnComplete();
    
    StorageEnvironment* env;
    StorageFileChunk*   writeChunk;
};

#endif
