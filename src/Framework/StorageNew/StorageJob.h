#ifndef STORAGEJOB_H
#define STORAGEJOB_H

#include "StorageChunk.h"

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
    StorageWriteChunkJob(StorageChunk* chunk_);
    
    void            Execute();
    
private:
    StorageChunk*   chunk;
};

#endif
