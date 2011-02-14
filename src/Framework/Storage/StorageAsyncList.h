#ifndef STORAGEASYNCLIST_H
#define STORAGEASYNCLIST_H

#include "System/Buffers/ReadBuffer.h"
#include "System/Events/Callable.h"
#include "System/Containers/List.h"

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
        FILE_CHUNK
    };
public:
    ReadBuffer              startKey;
    ReadBuffer              prefix;
    unsigned                count;
    bool                    forward;
    unsigned                offset;
    bool                    keyValues;
    
    bool                    ret;
    bool                    completed;
    Stage                   stage;
    Callable                onComplete;
    StorageShard*           shard;
    StorageChunk**          itChunk;
    StoragePage*            lastLoadedPage;
    ThreadPool*             threadPool;
    StorageChunkReader*     readers;
    unsigned                numReaders;
    StorageFileKeyValue**   iterators;
    ShardList               shards;

    StorageAsyncList();
    
    void                    ExecuteAsyncList();
};

#endif
