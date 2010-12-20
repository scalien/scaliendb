#ifndef STORAGEJOB_H
#define STORAGEJOB_H

#include "System/Events/Callable.h"
#include "StorageMemoChunk.h"
#include "StorageFileChunk.h"
#include "StorageLogSegment.h"

class StorageEnvironment;

/*
===============================================================================================

 StorageJob

===============================================================================================
*/

class StorageJob
{
public:
    virtual void Execute() = 0;

protected:
    Callable*           onComplete;
};

/*
===============================================================================================

 StorageSerializeChunkJob

===============================================================================================
*/

class StorageSerializeChunkJob : public StorageJob
{
public:
    StorageSerializeChunkJob(StorageMemoChunk* chunk, Callable* onComplete);
    
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
    StorageWriteChunkJob(StorageFileChunk* chunk, Callable* onComplete);
    
    void                Execute();
    
private:
    StorageFileChunk*   fileChunk;
    Callable*           onComplete;
};

/*
===============================================================================================

 StorageArchiveLogSegmentJob

===============================================================================================
*/

class StorageArchiveLogSegmentJob : public StorageJob
{
public:
    StorageArchiveLogSegmentJob(StorageEnvironment* env, StorageLogSegment* logSegment, Callable* onComplete);
    
    void                Execute();
    
private:
    StorageEnvironment* env;
    StorageLogSegment*  logSegment;
    Callable*           onComplete;
};

#endif
