#ifndef STORAGEENVIRONMENT_H
#define STORAGEENVIRONMENT_H

#include "System/Buffers/Buffer.h"
#include "System/Containers/InList.h"
#include "System/Containers/ArrayList.h"
#include "System/Events/Countdown.h"
#include "System/Threading/ThreadPool.h"
#include "System/Threading/JobProcessor.h"
#include "StorageConfig.h"
#include "StorageLogSegment.h"
#include "StorageMemoChunk.h"
#include "StorageFileChunk.h"
#include "StorageShard.h"
#include "StorageCommitJob.h"
#include "StorageBulkCursor.h"
#include "StorageAsyncBulkCursor.h"

class StorageRecovery;
class StorageEnvironmentWriter;
class StorageArchiveLogSegmentJob;
class StorageAsyncList;
class StorageSerializeChunkJob;
class StorageWriteChunkJob;
class StorageMergeChunkJob;
class StorageArchiveLogSegmentJob;

#define STORAGE_DEFAULT_MAX_UNBACKED_LOG_SEGMENT    10
#define STORAGE_DEFAULT_BACKGROUND_TIMER_DELAY      5  // sec

/*
===============================================================================================

 StorageEnvironment

===============================================================================================
*/

class StorageEnvironment
{
    friend class StorageRecovery;
    friend class StorageEnvironmentWriter;
    friend class StorageChunkSerializer;
    friend class StorageChunkWriter;
    friend class StorageChunkMerger;
    friend class StorageArchiveLogSegmentJob;
    friend class StorageBulkCursor;
    friend class StorageAsyncBulkCursor;
    
    typedef InList<StorageLogSegment>   LogSegmentList;
    typedef InList<StorageShard>        ShardList;
    typedef InList<StorageFileChunk>    FileChunkList;

public:
    typedef ArrayList<uint64_t, 64>     ShardIDList;

    StorageEnvironment();
    
    bool                    Open(Buffer& envPath);
    void                    Close();

    void                    SetYieldThreads(bool yieldThreads);
    void                    SetMergeEnabled(bool mergeEnabled);

    uint64_t                GetShardID(uint16_t contextID, uint64_t tableID, ReadBuffer& key);
    bool                    ShardExists(uint16_t contextID, uint64_t shardID);
    void                    GetShardIDs(uint64_t contextID, ShardIDList& shardIDs);
    void                    GetShardIDs(uint64_t contextID, uint64_t tableID, ShardIDList& shardIDs);

    bool                    CreateShard(uint16_t contextID, uint64_t shardID, uint64_t tableID,
                             ReadBuffer firstKey, ReadBuffer lastKey,
                             bool useBloomFilter, bool isLogStorage);
    bool                    DeleteShard(uint16_t contextID, uint64_t shardID);
    bool                    SplitShard(uint16_t contextID,  uint64_t shardID,
                             uint64_t newShardID, ReadBuffer splitKey);
                             
    bool                    Get(uint16_t contextID, uint64_t shardID, ReadBuffer key, ReadBuffer& value);
    bool                    Set(uint16_t contextID, uint64_t shardID, ReadBuffer key, ReadBuffer value);
    bool                    Delete(uint16_t contextID, uint64_t shardID, ReadBuffer key);

    bool                    TryNonblockingGet(uint16_t contextID, uint64_t shardID, StorageAsyncGet* asyncGet);
    void                    AsyncGet(uint16_t contextID, uint64_t shardID, StorageAsyncGet* asyncGet);
    void                    AsyncList(uint16_t contextID, uint64_t shardID, StorageAsyncList* asyncList);

    StorageBulkCursor*      GetBulkCursor(uint16_t contextID, uint64_t shardID);
    StorageAsyncBulkCursor* GetAsyncBulkCursor(uint16_t contextID, uint64_t shardID, Callable onResult);
    void                    DecreaseNumCursors();

    uint64_t                GetSize(uint16_t contextID, uint64_t shardID);
    ReadBuffer              GetMidpoint(uint16_t contextID, uint64_t shardID);
    bool                    IsSplitable(uint16_t contextID, uint64_t shardID);
    
    bool                    Commit();
    bool                    Commit(Callable& onCommit_);
    bool                    GetCommitStatus();
    bool                    IsCommiting();

    bool                    IsShuttingDown();
    
    void                    PrintState(uint16_t contextID, Buffer& buffer);
    StorageConfig&          GetConfig();
    
    void                    OnCommit(StorageCommitJob* job);
    void                    TryFinalizeLogSegment();
    void                    TrySerializeChunks();
    void                    TryWriteChunks();
    void                    TryMergeChunks();
    void                    TryArchiveLogSegments();
    void                    OnChunkSerialize(StorageSerializeChunkJob* job);
    void                    OnChunkWrite(StorageWriteChunkJob* job);
    void                    OnChunkMerge(StorageMergeChunkJob* job);
    void                    OnLogArchive(StorageArchiveLogSegmentJob* job);
    void                    OnBackgroundTimer();
    StorageShard*           GetShard(uint16_t contextID, uint64_t shardID);
    void                    WriteTOC();
    StorageFileChunk*       GetFileChunk(uint64_t chunkID);
    void                    EnqueueAsyncGet(StorageAsyncGet* asyncGet);
    void                    OnChunkSerialized(StorageMemoChunk* memoChunk, StorageFileChunk* fileChunk);

    Countdown               backgroundTimer;
    Callable                onBackgroundTimer;

    StorageLogSegment*      headLogSegment;
    ShardList               shards;
    FileChunkList           fileChunks;
    LogSegmentList          logSegments;
    StorageConfig           config;
    
    Callable                onCommit;
    JobProcessor            commitJobs;
    JobProcessor            serializeChunkJobs;
    JobProcessor            writeChunkJobs;
    JobProcessor            mergeChunkJobs;
    JobProcessor            archiveLogJobs;
    JobProcessor            deleteChunkJobs;
    ThreadPool*             asyncThread;
    ThreadPool*             asyncGetThread;

    Buffer                  envPath;
    Buffer                  chunkPath;
    Buffer                  logPath;
    Buffer                  archivePath;

    uint64_t                nextChunkID;
    uint64_t                nextLogSegmentID;
    uint64_t                lastWriteTime;
    unsigned                numCursors;
    const char*             archiveScript;
    bool                    yieldThreads;
    bool                    shuttingDown;
    bool                    writingTOC;
    bool                    mergeEnabled;
    
    unsigned                maxUnbackedLogSegments;
};

#endif
