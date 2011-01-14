#ifndef STORAGERECOVERY_H
#define STORAGERECOVERY_H

#include "StorageEnvironment.h"

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
    bool                    ReadShards(ReadBuffer& parse);
    bool                    ReadShard(ReadBuffer& parse);
    void                    CreateMemoChunks();
    void                    ReadFileChunks();
    void                    ComputeShardRecovery();
    void                    ReplayLogSegments();
    bool                    ReplayLogSegment(Buffer& filename);
    void                    DeleteOrphanedChunks();
    
    void                    ExecuteSet(
                             uint64_t logSegmentID, uint32_t logCommandID,
                             uint16_t contextID, uint64_t shardID,
                             ReadBuffer& key, ReadBuffer& value);

    void                    ExecuteDelete(
                             uint64_t logSegmentID, uint32_t logCommandID,
                             uint16_t contextID, uint64_t shardID,
                             ReadBuffer& key);

    StorageEnvironment*     env;
};

#endif
