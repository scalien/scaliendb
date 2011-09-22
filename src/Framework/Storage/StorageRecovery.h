#ifndef STORAGERECOVERY_H
#define STORAGERECOVERY_H

#include "StorageEnvironment.h"

#define STORAGE_RECOVERY_PRELOAD_SIZE   (1024*1024)

class StorageEnvironment;

/*
===============================================================================================

 StorageRecovery

===============================================================================================
*/

class StorageRecovery
{
public:
    bool                    TryRecovery(StorageEnvironment* env);
    
private:
    bool                    TryReadTOC(Buffer& filename);
    bool                    ReadShards(uint32_t version, ReadBuffer& parse);
    bool                    ReadShardVersion1(ReadBuffer& parse);
    void                    CreateMemoChunks();
    void                    ReadFileChunks();
    void                    ComputeShardRecovery();
    ReadBuffer              ReadFromFileBuffer(FD fd, uint64_t len);
    void                    ReplayLogSegments(uint64_t trackID);
    bool                    ReplayLogSegment(uint64_t trackID, Buffer& filename);
    void                    DeleteOrphanedChunks();
    
    void                    ExecuteSet(
                             uint64_t logSegmentID, uint32_t logCommandID,
                             uint16_t contextID, uint64_t shardID,
                             ReadBuffer& key, ReadBuffer& value);

    void                    ExecuteDelete(
                             uint64_t logSegmentID, uint32_t logCommandID,
                             uint16_t contextID, uint64_t shardID,
                             ReadBuffer& key);
    
    void                    TryWriteChunks();

    StorageEnvironment*     env;
    Buffer                  fileBuffer;
    uint64_t                fileBufferPos;
};

#endif
