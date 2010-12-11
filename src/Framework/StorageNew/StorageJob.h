#ifndef STORAGEJOB_H
#define STORAGEJOB_H

#include "StorageFileChunk.h"

/*
===============================================================================================

 StorageJob

===============================================================================================
*/

class StorageJob
{
public:
    virtual void Execute() = 0;
};

class StorageChunk; // forward

/*
===============================================================================================

 StorageWriteChunkJob

===============================================================================================
*/

class StorageWriteChunkJob : public StorageJob
{
public:
    StorageWriteChunkJob(StorageFileChunk* chunk_);
    
    void                Execute();
    
private:
    StorageFileChunk*   fileChunk;
};

#endif
