#ifndef STORAGEASYNCLIST_H
#define STORAGEASYNCLIST_H

#include "System/Buffers/ReadBuffer.h"
#include "System/Events/Callable.h"
#include "System/Containers/List.h"
#include "StorageChunkLister.h"

class StorageShard;
class StorageChunk;
class StorageChunkReader;
class StorageFileKeyValue;
class StoragePage;
class ThreadPool;

/*
===============================================================================================

 StorageAsyncList

===============================================================================================
*/

class StorageAsyncList
{
    typedef List<StorageShard*> ShardList;
    enum Stage
    {
        START,
        MEMO_CHUNK,
        FILE_CHUNK,
        MERGE
    };
public:
    ReadBuffer              startKey;
    ReadBuffer              prefix;
    unsigned                count;
    unsigned                offset;
    bool                    keyValues;
    
    bool                    ret;
    bool                    completed;
    Stage                   stage;
    Callable                onComplete;
    StorageShard*           shard;
    StoragePage*            lastLoadedPage;
    ThreadPool*             threadPool;
    StorageFileKeyValue**   iterators;
    StorageChunkLister**    listers;
    unsigned                numListers;
    ShardList               shards;

    StorageAsyncList();
    
    void                    ExecuteAsyncList();
    void                    LoadMemoChunk();
    void                    AsyncLoadFileChunks();
    void                    AsyncMergeResult();
    StorageFileKeyValue*    Next(ReadBuffer& lastKey);
};

#endif
