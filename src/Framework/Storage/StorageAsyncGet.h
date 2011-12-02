#ifndef STORAGEASYNCGET_H
#define STORAGEASYNCGET_H

#include "System/Buffers/ReadBuffer.h"
#include "System/Events/Callable.h"
#include "StorageFileChunk.h"

class StorageEnvironment;
class StorageShard;
class StorageFileChunk;
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
    bool                skipMemoChunk;
    Stage               stage;
    Callable            onComplete;
    uint32_t            index;
    uint64_t            offset;
    StoragePage*        lastLoadedPage;
    ThreadPool*         threadPool;
    uint16_t            contextID;
    uint64_t            shardID;
    uint64_t            chunkID;
    StorageEnvironment* env;
    StorageFileChunk    loaderFileChunk;
    
    StorageAsyncGet();

	StorageChunk**		GetChunkIterator(StorageShard* shard);
    void                ExecuteAsyncGet();
    void                SetLastLoadedPage(StorageFileChunk* fileChunk);
    void                SetupLoaderFileChunk(StorageFileChunk* fileChunk);
    void                OnComplete();
    void                AsyncLoadPage();
};

#endif
