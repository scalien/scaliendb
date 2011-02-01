#ifndef STORAGEENVIRONMENT_H
#define STORAGEENVIRONMENT_H

#include "System/Buffers/Buffer.h"
#include "System/Containers/InList.h"
#include "System/Events/Countdown.h"
#include "System/ThreadPool.h"
#include "StorageConfig.h"
#include "StorageLogSegment.h"
#include "StorageMemoChunk.h"
#include "StorageFileChunk.h"
#include "StorageShard.h"
#include "StorageJob.h"
#include "StorageBulkCursor.h"

class StorageRecovery;
class StorageEnvironment;
class StorageEnvironmentWriter;
class StorageArchiveLogSegmentJob;

#define STORAGE_MAX_UNBACKED_LOG_SEGMENT    10
#define STORAGE_BACKGROUND_TIMER_DELAY      10*1000 // msec
#define STORAGE_MERGE_AFTER_WRITE_DELAY     60*1000 // msec

/*
===============================================================================================

 StorageEnvironment

// TODO:        cursor

===============================================================================================
*/

class StorageEnvironment
{
    friend class StorageRecovery;
    friend class StorageEnvironmentWriter;
    friend class StorageChunkWriter;
    friend class StorageChunkMerger;
    friend class StorageArchiveLogSegmentJob;
    friend class StorageBulkCursor;
    
    typedef InList<StorageLogSegment>   LogSegmentList;
    typedef InList<StorageShard>        ShardList;
    typedef InList<StorageFileChunk>    FileChunkList;
    typedef InList<StorageAsyncGet>     AsyncGetList;

public:
    StorageEnvironment();
    
    bool                    Open(Buffer& envPath);
    void                    Close();

    void                    SetYieldThreads(bool yieldThreads);

    uint64_t                GetShardID(uint16_t contextID, uint64_t tableID, ReadBuffer& key);

    bool                    CreateShard(uint16_t contextID, uint64_t shardID, uint64_t tableID,
                             ReadBuffer firstKey, ReadBuffer lastKey,
                             bool useBloomFilter, bool isLogStorage);
    bool                    DeleteShard(uint16_t contextID, uint64_t shardID);
    bool                    SplitShard(uint16_t contextID,  uint64_t shardID,
                             uint64_t newShardID, ReadBuffer splitKey);

    bool                    Get(uint16_t contextID, uint64_t shardID, ReadBuffer key, ReadBuffer& value);
    bool                    Set(uint16_t contextID, uint64_t shardID, ReadBuffer key, ReadBuffer value);
    bool                    Delete(uint16_t contextID, uint64_t shardID, ReadBuffer key);

    void                    AsyncGet(uint16_t contextID, uint64_t shardID, StorageAsyncGet* asyncGet);

    StorageBulkCursor*      GetBulkCursor(uint16_t contextID, uint64_t shardID);

    uint64_t                GetSize(uint16_t contextID, uint64_t shardID);
    ReadBuffer              GetMidpoint(uint16_t contextID, uint64_t shardID);
    bool                    IsSplittable(uint16_t contextID, uint64_t shardID);
    
    bool                    Commit();
    bool                    Commit(Callable& onCommit_);
    bool                    GetCommitStatus();
    bool                    IsCommiting();
    
private:
    void                    OnCommit();
    void                    TryFinalizeLogSegment();
    void                    TrySerializeChunks();
    void                    TryWriteChunks();
    void                    TryMergeChunks();
    void                    TryArchiveLogSegments();
    void                    OnChunkSerialize();
    void                    OnChunkWrite();
    void                    OnChunkMerge();
    void                    OnLogArchive();
    void                    OnBackgroundTimer();
    StorageShard*           GetShard(uint16_t contextID, uint64_t shardID);
    void                    StartJob(ThreadPool* thread, StorageJob* job);
    void                    WriteTOC();
    StorageFileChunk*       GetFileChunk(uint64_t chunkID);
    void                    EnqueueAsyncGet(StorageAsyncGet* asyncGet);

    Countdown               backgroundTimer;
    Callable                onBackgroundTimer;

    Callable                onCommitCallback;
    Callable                onCommit;
    Callable                onChunkSerialize;
    Callable                onChunkWrite;
    Callable                onChunkMerge;
    Callable                onLogArchive;

    StorageLogSegment*      headLogSegment;
    StorageMemoChunk*       serializeChunk;
    StorageFileChunk*       writeChunk;
    StorageFileChunk*       mergeChunkIn1;
    StorageFileChunk*       mergeChunkIn2;
    StorageFileChunk*       mergeChunkOut;
    ShardList               shards;
    FileChunkList           fileChunks;
    LogSegmentList          logSegments;
    StorageConfig           config;
    
    ThreadPool*             commitThread;
    bool                    commitThreadActive;
    ThreadPool*             serializerThread;
    bool                    serializerThreadActive;
    ThreadPool*             writerThread;
    bool                    writerThreadActive;
    ThreadPool*             archiverThread;
    bool                    archiverThreadActive;
    ThreadPool*             asyncThread;
    ThreadPool*             asyncGetThread;

    Buffer                  envPath;
    Buffer                  chunkPath;
    Buffer                  logPath;
    Buffer                  archivePath;

    uint64_t                nextChunkID;
    uint64_t                nextLogSegmentID;
    uint64_t                asyncLogSegmentID;
    uint64_t                asyncWriteChunkID;
    uint16_t                mergeContextID;
    uint64_t                mergeShardID;
    uint64_t                lastWriteTime;
    bool                    numBulkCursors;
    const char*             archiveScript;
    bool                    haveUncommitedWrites;
    bool                    yieldThreads;
};

#endif
