#ifndef STORAGEASYNCLIST_H
#define STORAGEASYNCLIST_H

#include "System/Buffers/ReadBuffer.h"
#include "System/Events/Callable.h"
#include "System/Containers/List.h"
#include "StorageChunkLister.h"
#include "StorageDataPage.h"

class StorageShard;
class StorageChunk;
class StorageChunkReader;
class StorageFileKeyValue;
class StoragePage;
class StorageAsyncList;
class StorageEnvironment;
class ThreadPool;

/*
===============================================================================================

 StorageAsyncListResult

===============================================================================================
*/

class StorageAsyncListResult
{
public:
    StorageAsyncList*   asyncList;
    StorageDataPage     dataPage;
    unsigned            numKeys;
    bool                final;
    Callable            onComplete;

    StorageAsyncListResult(StorageAsyncList* asyncList);
    
    void                OnComplete();
    void                Append(StorageFileKeyValue* kv);
    uint32_t            GetSize();
};

/*
===============================================================================================

 StorageAsyncList

===============================================================================================
*/

class StorageAsyncList
{
    typedef List<StorageShard*> ShardList;
public:
    enum Stage
    {
        START,
        MEMO_CHUNK,
        FILE_CHUNK,
        MERGE
    };
    enum Type
    {
        KEY,
        KEYVALUE,
        COUNT
    };

    unsigned                count;
    ReadBuffer              startKey;
    ReadBuffer              endKey;
    ReadBuffer              prefix;
    Type                    type;
    bool                    forwardDirection;
    bool                    startWithLastKey;
    
    bool                    ret;
    bool                    completed;
    bool                    aborted;
    unsigned                num;
    Stage                   stage;
    Callable                onComplete;
    StorageShard*           shard;
    Buffer                  shardFirstKey;
    Buffer                  shardLastKey;
    ThreadPool*             threadPool;
    StorageFileKeyValue**   iterators;
    StorageChunkLister**    listers;
    unsigned                numListers;
    StorageAsyncListResult* lastResult;
    StorageEnvironment*     env;
    uint64_t                requestID;

    StorageAsyncList();
    
    void                    Init();
    void                    Clear();
    void                    ExecuteAsyncList();
    void                    LoadMemoChunk(bool keysOnly);
    void                    AsyncLoadChunks();
    void                    AsyncMergeResult();
    void                    OnResult(StorageAsyncListResult* result);
    bool                    IsDone();
    bool                    IsAborted();
    void                    SetAborted(bool aborted);
    bool                    IsKeyInShard(const ReadBuffer& key);
    int                     CompareSmallestKey(const ReadBuffer& key, const ReadBuffer& smallestKey);
    StorageFileKeyValue*    GetSmallest();
    StorageFileKeyValue*    Next();
};

#endif
