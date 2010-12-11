#ifndef STORAGEJOB_H
#define STORAGEJOB_H

#include "StorageChunkFile.h"

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
    StorageWriteChunkJob(StorageChunkFile* chunk_);
    
    void                Execute();
    
private:
    StorageChunkFile*   fileChunk;
};

#endif
