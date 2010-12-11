#ifndef STORAGEENVIRONMENT_H
#define STORAGEENVIRONMENT_H

#include "System/Buffers/Buffer.h"
#include "System/Containers/InList.h"
#include "System/Events/Countdown.h"
#include "System/Threadpool.h"
#include "StorageConfig.h"
#include "StorageLogSegmentWriter.h"
#include "StorageMemoChunk.h"
#include "StorageFileChunk.h"
#include "StorageShard.h"
#include "StorageJob.h"

/*
===============================================================================================

 StorageEnvironment

// TODO:        cursor
// TODO:        create shard (w/ tableID)
// TODO:        split shard

===============================================================================================
*/

class StorageEnvironment
{
//    typedef InList<StorageLogSegment> LogSegmentList;
    typedef InList<StorageShard>        ShardList;
    typedef InList<StorageMemoChunk>    MemoChunkList;
    typedef InList<StorageFileChunk>    FileChunkList;

public:
    StorageEnvironment();
    
    bool                        Open(Buffer& envPath);
    void                        Close();

    void                        SetStorageConfig(StorageConfig& config);

    uint64_t                    GetShardID(uint64_t tableID, ReadBuffer& key);

    void                        CreateShard(uint64_t shardID, uint64_t tableID,
                                 ReadBuffer& firstKey, ReadBuffer& lastKey, bool useBloomFilter);
    void                        DeleteShard(uint64_t shardID);

    bool                        Get(uint64_t shardID, ReadBuffer& key, ReadBuffer& value);
    bool                        Set(uint64_t shardID, ReadBuffer& key, ReadBuffer& value);
    bool                        Delete(uint64_t shardID, ReadBuffer& key);
        
    void                        SetOnCommit(Callable& onCommit);
    bool                        Commit();
    bool                        GetCommitStatus();

private:
    void                        OnCommit();
    void                        OnBackgroundTimeout();
    void                        TryFinalizeLogSegment();
    void                        TryFinalizeChunks();
    bool                        IsWriteActive();
    StorageShard*               GetShard(uint64_t shardID);
    void                        StartJob(StorageJob* job);

    Callable                    onCommit;

    StorageLogSegmentWriter*    logSegmentWriter;
    ShardList                   shards;
    FileChunkList               fileChunks;
//    LogSegmentList            logSegments;

    StorageConfig               config;
    
    ThreadPool*                 commitThread;
    ThreadPool*                 backgroundThread;
    Countdown                   backgroundTimer;

    Buffer                      envPath;
    Buffer                      chunkPath;
    Buffer                      logPath;

    uint64_t                    nextChunkID;
    uint64_t                    nextLogSegmentID;
};

#endif
