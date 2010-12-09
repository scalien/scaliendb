#ifndef STORAGEENVIRONMENT_H
#define STORAGEENVIRONMENT_H

#include "System/Buffers/Buffer.h"
#include "System/Threadpool.h"

/*
===============================================================================================

 StorageEnvironment

===============================================================================================
*/

class StorageEnvironment
{
    typedef InList<StorageLogSegemnt> LogSegmentList
    typedef InList<StorageShard> ShardList;
    typedef InList<StorageChunk> ChunkList;

public:
    StorageEnvironment();
    
    bool                Open(const char* filepath);
    void                Close();

    void                SetStorageConfig(StorageConfig& config);

    uint64_t            GetShardID(uint64_t tableID, ReadBuffer& key);
    bool                Get(uint64_t shardID, ReadBuffer& key, ReadBuffer& value);
    bool                Set(uint64_t shardID, ReadBuffer& key, ReadBuffer& value);
    bool                Delete(uint64_t shardID, ReadBuffer& key);
        
    void                SetOnCommit(Callable& onCommit);
    void                Commit();
    bool                GetCommitStatus();

    // TODO:        cursor
    // TODO:        create shard (w/ tableID)
    // TODO:        split shard

private:
    void                OnCommit();
    void                OnBackgroundTimeout();
    void                TryFinalizeLogSegment();
    void                TryFinalizeChunks();
    bool                IsWriteActive();
    StorageShard*       GetShard(uint64_t shardID);
    StorageChunk*       GetChunk(uint64_t chunkID);
    void                StartJob(StorageJob* job);

    Callable            onCommit;

    StorageLogSegment*  headLogSegment;
    ShardList           shards;
    ChunkList           chunks;
    LogSegmentList      logSegments;

    StorageConfig       config;
    
    ThreadPool*         commitThread;
    ThreadPool*         backgroundThread;
    Countdown           backgroundTimer;
};

#endif
