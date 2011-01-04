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
class StorageEnvironmentWriter;
class StorageArchiveLogSegmentJob;

#define STORAGE_MAX_UNBACKED_LOG_SEGMENT    10

/*
===============================================================================================

 StorageEnvironment

// TODO:        cursor
// TODO:        split shard

===============================================================================================
*/

class StorageEnvironment
{
    friend class StorageRecovery;
    friend class StorageEnvironmentWriter;
    friend class StorageArchiveLogSegmentJob;
    
    typedef InList<StorageLogSegment>   LogSegmentList;
    typedef InList<StorageShard>        ShardList;
    typedef InList<StorageMemoChunk>    MemoChunkList;
    typedef InList<StorageFileChunk>    FileChunkList;

public:
    StorageEnvironment();
    
    bool                    Open(Buffer& envPath);
    void                    Close();

    void                    SetStorageConfig(StorageConfig& config);

    uint64_t                GetShardID(uint16_t contextID, uint64_t tableID, ReadBuffer& key);

    bool                    CreateShard(uint16_t contextID, uint64_t shardID, uint64_t tableID,
                             ReadBuffer firstKey, ReadBuffer lastKey, bool useBloomFilter);
    bool                    DeleteShard(uint16_t contextID, uint64_t shardID);

    bool                    Get(uint16_t contextID, uint64_t shardID, ReadBuffer key, ReadBuffer& value);
    bool                    Set(uint16_t contextID, uint64_t shardID, ReadBuffer key, ReadBuffer value);
    bool                    Delete(uint16_t contextID, uint64_t shardID, ReadBuffer key);

    StorageBulkCursor*      GetBulkCursor(uint16_t contextID, uint64_t shardID);
    
    void                    SetOnCommit(Callable& onCommit);
    bool                    Commit();
    bool                    GetCommitStatus();
    
private:
    void                    OnCommit();
    void                    TryFinalizeLogSegment();
    void                    TrySerializeChunks();
    void                    TryWriteChunks();
    void                    TryArchiveLogSegments();
    void                    OnChunkSerialize();
    void                    OnChunkWrite();
    void                    OnLogArchive();
    bool                    IsWriteActive();
    StorageShard*           GetShard(uint16_t contextID, uint64_t shardID);
    void                    StartJob(ThreadPool* thread, StorageJob* job);
    void                    WriteTOC();
    StorageFileChunk*       GetFileChunk(uint64_t chunkID);

    Callable                onCommit;
    Callable                onChunkSerialize;
    Callable                onChunkWrite;
    Callable                onLogArchive;

    StorageLogSegment*      headLogSegment;
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

    Buffer                  envPath;
    Buffer                  chunkPath;
    Buffer                  logPath;
    Buffer                  archivePath;

    uint64_t                nextChunkID;
    uint64_t                nextLogSegmentID;
    uint64_t                asyncLogSegmentID;
    uint64_t                asyncWriteChunkID;
    bool                    asyncCommit;
    const char*             archiveScript;
};

#endif
