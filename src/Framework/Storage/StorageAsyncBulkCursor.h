#ifndef STORAGEASYNCBULKCURSOR_H
#define STORAGEASYNCBULKCURSOR_H

#include "System/Events/Callable.h"
#include "System/ThreadPool.h"
#include "StorageFileKeyValue.h"
#include "StorageChunk.h"
#include "StorageShard.h"

class StorageEnvironment;
class StorageMemoChunk;
class StorageFileChunk;
class StorageAsyncBulkCursor;

/*
===============================================================================================

 StorageAsyncBulkResult

===============================================================================================
*/

class StorageAsyncBulkResult
{
public:
    StorageAsyncBulkResult(StorageAsyncBulkCursor* cursor);

    void                    OnComplete();

    bool                    last;
    StorageAsyncBulkCursor* cursor;
    Callable                onComplete;
    StorageDataPage         dataPage;
};

/*
===============================================================================================

 StorageAsyncBulkCursor

===============================================================================================
*/

class StorageAsyncBulkCursor
{
    friend class StorageAsyncBulkResult;
public:
    StorageAsyncBulkCursor();

    void                    OnNextChunk();
    
    void                    SetEnvironment(StorageEnvironment* env);
    void                    SetShard(uint64_t contextID_, uint64_t shardID);
    void                    SetThreadPool(ThreadPool* threadPool_);
    void                    SetOnComplete(Callable onComplete);
    
    StorageAsyncBulkResult* GetLastResult();    
    
private:
    void                    AsyncReadFileChunk();
    void                    TransferDataPage(StorageAsyncBulkResult* result, StorageDataPage* page);
    void                    OnResult(StorageAsyncBulkResult* result);
    
    Buffer                  chunkName;
    Callable                onComplete;
    StorageShard*           shard;
    StorageChunk**          itChunk;
    ThreadPool*             threadPool;
    StorageEnvironment*     env;
    StorageAsyncBulkResult* lastResult;
};

#endif
