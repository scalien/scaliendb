#ifndef STORAGEJOB_H
#define STORAGEJOB_H

#include "StorageMemoChunk.h"
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

/*
===============================================================================================

 StorageSerializeChunkJob

===============================================================================================
*/

class StorageSerializeChunkJob : public StorageJob
{
public:
    StorageSerializeChunkJob(StorageMemoChunk* chunk_);
    
    void                Execute();
    
private:
    StorageMemoChunk*   memoChunk;
};

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
