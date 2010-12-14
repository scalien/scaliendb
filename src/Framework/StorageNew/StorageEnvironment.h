#ifndef STORAGEENVIRONMENT_H
#define STORAGEENVIRONMENT_H

#include "System/Buffers/Buffer.h"
#include "System/Containers/InList.h"
#include "System/Events/Countdown.h"
#include "System/Threadpool.h"
#include "StorageConfig.h"
#include "StorageLogSegment.h"
#include "StorageMemoChunk.h"
#include "StorageFileChunk.h"
#include "StorageShard.h"
#include "StorageJob.h"

class StorageEnvironmentWriter;

/*
===============================================================================================

 StorageEnvironment

// TODO:        cursor
// TODO:        split shard

===============================================================================================
*/

class StorageEnvironment
{
    friend class StorageEnvironmentWriter;
//    typedef InList<StorageLogSegment> LogSegmentList;
    typedef InList<StorageShard>        ShardList;
    typedef InList<StorageMemoChunk>    MemoChunkList;
    typedef InList<StorageFileChunk>    FileChunkList;

public:
    StorageEnvironment();
    
    bool                        Open(Buffer& envPath);
    void                        Close();

    void                        SetStorageConfig(StorageConfig& config);

    uint64_t                    GetShardID(uint16_t contextID, uint64_t tableID, ReadBuffer& key);

    void                        CreateShard(uint16_t contextID, uint64_t shardID, uint64_t tableID,
                                 ReadBuffer firstKey, ReadBuffer lastKey, bool useBloomFilter);
    void                        DeleteShard(uint16_t contextID, uint64_t shardID);

    bool                        Get(uint16_t contextID, uint64_t shardID, ReadBuffer key, ReadBuffer& value);
    bool                        Set(uint16_t contextID, uint64_t shardID, ReadBuffer key, ReadBuffer value);
    bool                        Delete(uint16_t contextID, uint64_t shardID, ReadBuffer key);
        
    void                        SetOnCommit(Callable& onCommit);
    bool                        Commit();
    bool                        GetCommitStatus();

private:
    void                        OnCommit();
    void                        TryFinalizeLogSegment();
    void                        TrySerializeChunks();
    void                        TryWriteChunks();
    void                        OnChunkSerialize();
    void                        OnChunkWrite();
    bool                        IsWriteActive();
    StorageShard*               GetShard(uint16_t contextID, uint64_t shardID);
    void                        StartJob(ThreadPool* thread, StorageJob* job);
    void                        WriteTOC();

    Callable                    onCommit;
    Callable                    onChunkSerialize;
    Callable                    onChunkWrite;

    StorageLogSegment*    logSegmentWriter;
    ShardList                   shards;
    FileChunkList               fileChunks;
//    LogSegmentList            logSegments;

    StorageConfig               config;
    
    ThreadPool*                 commitThread;
    ThreadPool*                 serializerThread;
    ThreadPool*                 writerThread;

    Buffer                      envPath;
    Buffer                      chunkPath;
    Buffer                      logPath;

    uint64_t                    nextChunkID;
    uint64_t                    nextLogSegmentID;
    bool                        asyncCommit;
};

#endif
