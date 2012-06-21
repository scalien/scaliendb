#ifndef STORAGEENVIRONMENT_H
#define STORAGEENVIRONMENT_H

#include "System/Registry.h"
#include "System/Buffers/Buffer.h"
#include "System/Containers/InList.h"
#include "System/Containers/ArrayList.h"
#include "System/Containers/HashMap.h"
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
#include "StorageLogManager.h"

class StorageRecovery;
class StorageEnvironmentWriter;
class StorageArchiveLogSegmentJob;
class StorageAsyncList;
class StorageSerializeChunkJob;
class StorageWriteChunkJob;
class StorageMergeChunkJob;
class StorageArchiveLogSegmentJob;

#define STORAGE_DEFAULT_BACKGROUND_TIMER_DELAY      1  // sec

// TODO: we found that Windows and Linux behaves differently when writing to disk
// On Linux, it is preferred that you write and sync to disk as often as you can (e.g. pageSize or 64K)
// On Windows, it is preferred that you write less often but in bigger chunks
#ifdef PLATFORM_WINDOWS
#define STORAGE_WRITE_GRANULARITY                   (2*MiB)
#else
#define STORAGE_WRITE_GRANULARITY                   (64*KiB)
#endif

#define STORAGE_DEFAULT_MERGE_CPU_THRESHOLD         (50)

struct ShardSize;

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
    
    typedef InList<StorageShard> ShardList;
    typedef InList<StorageFileChunk> FileChunkList;
    typedef StorageLogManager LogManager;
    typedef LogManager::Track Track;
    typedef List<Track> TrackList;

public:
    StorageEnvironment();
    
    bool                    Open(Buffer& envPath, StorageConfig config);
    void                    Close();

    static void             Sync(FD fd);

    void                    SetMergeEnabled(bool mergeEnabled);
    void                    SetMergeCpuThreshold(uint32_t mergeCpuThreshold);
    uint32_t                GetMergeCpuThreshold();
    void                    SetDeleteEnabled(bool deleteEnabled);

    uint64_t                GetShardID(uint16_t contextID, uint64_t tableID, ReadBuffer& key);
    uint64_t                GetShardIDByLastKey(uint16_t contextID, uint64_t tableID, ReadBuffer& key);
    bool                    ShardExists(uint16_t contextID, uint64_t shardID);
    void                    GetShardIDs(uint64_t contextID, Buffer& shardIDs);
    void                    GetShardIDs(uint64_t contextID, uint64_t tableID, Buffer& shardIDs);

    bool                    CreateShard(uint64_t trackID,
                             uint16_t contextID, uint64_t shardID, uint64_t tableID,
                             ReadBuffer firstKey, ReadBuffer lastKey,
                             bool useBloomFilter, char storageType);
    void                    DeleteShard(uint16_t contextID, uint64_t shardID, bool bulkDelete = false);
    bool                    SplitShard(uint16_t contextID,  uint64_t shardID,
                             uint64_t newShardID, ReadBuffer splitKey);

    bool                    DeleteTrack(uint64_t trackID);
                             
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
    
    bool                    Commit(uint64_t trackID);
    bool                    Commit(uint64_t trackID, Callable& onCommit_);
    bool                    IsCommitting(uint64_t trackID);
    
    bool                    PushMemoChunk(uint16_t contextID, uint64_t shardID);
    void                    DumpMemoChunks();

    bool                    IsShuttingDown();
    bool                    IsMergeEnabled();
    bool                    IsMergeStarted();
    bool                    IsMergeRunning();

    void                    PrintState(uint16_t contextID, Buffer& buffer);
    uint64_t                GetShardMemoryUsage();
    uint64_t                GetLogSegmentMemoryUsage();
    uint64_t                GetChunkFileDiskUsage();
    uint64_t                GetLogSegmentDiskUsage();
    unsigned                GetNumShards();
    unsigned                GetNumFileChunks();
    unsigned                GetNumListThreads();
    unsigned                GetNumActiveListThreads();
    unsigned                GetNumFinishedMergeJobs();
    StorageConfig&          GetConfig();
    
    void                    OnCommit(StorageCommitJob* job);
    void                    TryFinalizeLogSegments();
    void                    TrySerializeChunks();
    void                    TryWriteChunks();
    void                    TryMergeChunks();
    void                    TryArchiveLogSegments();
    void                    TryDeleteFileChunks();
    bool                    TryDeleteLogTypeFileChunk(StorageShard* shard);
    bool                    TryDeleteDumpTypeFileChunk(StorageShard* shard);
    void                    OnChunkSerialize(StorageSerializeChunkJob* job);
    void                    OnChunkWrite(StorageWriteChunkJob* job);
    void                    OnChunkMerge(StorageMergeChunkJob* job);
    void                    OnLogArchive(StorageArchiveLogSegmentJob* job);
    void                    OnBackgroundTimer();
    StorageShard*           GetShard(uint16_t contextID, uint64_t shardID);
    StorageShard*           GetShardByKey(uint16_t contextID, uint64_t tableID, ReadBuffer& key);
    void                    WriteTOC();
    uint64_t                WriteSnapshotTOC(Buffer& configStateBuffer);
    void					WriteConfigStateFile(Buffer& configStateBuffer, uint64_t tocID);
    bool                    DeleteSnapshotTOC(uint64_t tocID);
    StorageFileChunk*       GetFileChunk(uint64_t chunkID);
    void                    EnqueueAsyncGet(StorageAsyncGet* asyncGet);
    void                    OnChunkSerialized(StorageMemoChunk* memoChunk, StorageFileChunk* fileChunk);
    unsigned                GetNumShards(StorageChunk* chunk);
    StorageShard*           GetFirstShard(StorageChunk* chunk);
    void                    ConstructShardSizes(InSortedList<ShardSize>& shardSizes);
    StorageShard*           FindLargestShardCond(
                             InSortedList<ShardSize>& shardSizes,
                             StorageShard::IsMergeCandidateFunc IsMergeCandidateFunc);
    void                    MergeChunk(StorageShard* shard);

    Buffer                  envPath;
    Buffer                  chunkPath;
    Buffer                  logPath;
    Buffer                  archivePath;

private:
    ShardList               shards;
    FileChunkList           fileChunks;
    StorageConfig           config;
    LogManager              logManager;

    Countdown               backgroundTimer;
    Callable                onBackgroundTimer;

    JobProcessor            commitJobs;
    JobProcessor            serializeChunkJobs;
    JobProcessor            writeChunkJobs;
    JobProcessor            mergeChunkJobs;
    JobProcessor            archiveLogJobs;
    JobProcessor            deleteChunkJobs;
    ThreadPool*             asyncListThread;
    ThreadPool*             asyncGetThread;

    uint64_t                nextChunkID;
    int                     mergeEnabledCounter; // enabled if > 0
    uint32_t                mergeCpuThreshold;   // only merge if CPU % is below this number
    unsigned                numFinishedMergeJobs;
    unsigned                numCursors;
    const char*             archiveScript;
    bool                    shuttingDown;
    bool                    writingTOC;
    bool                    dumpMemoChunks;
    uint64_t*               numWriteToc100;
    uint64_t*               numWriteToc1000;
};

#endif
