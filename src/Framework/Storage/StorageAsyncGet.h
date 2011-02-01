#ifndef STORAGEASYNCGET_H
#define STORAGEASYNCGET_H

#include "System/Buffers/ReadBuffer.h"
#include "System/Events/Callable.h"

class StorageShard;
class StorageChunk;
class StoragePage;
class ThreadPool;

/*
===============================================================================================

 StorageAsyncGet

===============================================================================================
*/

class StorageAsyncGet
{
public:
    enum Stage
    {
        START,
        INDEX_PAGE,
        BLOOM_PAGE,
        DATA_PAGE
    };
    ReadBuffer          key;
    ReadBuffer          value;
    bool                ret;
    bool                completed;
    Stage               stage;
    Callable            onComplete;
    uint32_t            index;
    uint32_t            offset;
    StorageShard*       shard;
    StorageChunk**      itChunk;
    StoragePage*        lastLoadedPage;
    ThreadPool*         threadPool;
    
    StorageAsyncGet();
    
    void                ExecuteAsyncGet();
    void                OnComplete();
    void                AsyncLoadPage();
};

#endif
