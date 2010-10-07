#ifndef STORAGETABLE_H
#define STORAGETABLE_H

#include "System/Containers/InTreeMap.h"
#include "System/IO/FD.h"
#include "StorageShard.h"
#include "StorageShardIndex.h"

class StorageDatabase;  // forward

/*
===============================================================================

 StorageTable

===============================================================================
*/

class StorageTable
{
public:
    typedef InTreeMap<StorageShardIndex> StorageIndexMap;

    const char*         GetName();
    uint64_t            GetSize();
    StorageDatabase*    GetDatabase();
    
    void                Open(const char* path, const char* name);
    void                Commit(bool recovery = true, bool flush = true);
    void                Close();
        
    bool                Get(ReadBuffer key, ReadBuffer& value);
    bool                Set(ReadBuffer key, ReadBuffer value, bool copy = true);
    bool                Delete(ReadBuffer key);

    bool                CreateShard(uint64_t shardID, ReadBuffer& startKey);
    StorageShard*       GetShard(uint64_t shardID);
    bool                SplitShard(uint64_t oldShardID, uint64_t newShardID, ReadBuffer& startKey);
    
    bool                ShardExists(ReadBuffer& startKey);
    // TODO:
    //bool              DeleteShard(uint64_t shardID);
    //bool              DeleteAllShards();

    StorageTable*       next;
    StorageTable*       prev;

private:
    StorageShardIndex*  Locate(ReadBuffer& key);
    void                PerformRecovery(uint64_t length);
    void                PerformRecoveryCreateShard(uint64_t& oldShardID, uint64_t& newShardID);
    void                PerformRecoveryCopy();
    void                PerformRecoveryMove();
    void                ReadTOC(uint64_t length);
    void                RebuildTOC();
    void                WriteTOC();
    void                WriteRecoveryDone();
    void                WriteRecoveryCreateShard(uint64_t oldShardID, uint64_t newShardID);
    void                WriteRecoveryCopy(uint64_t oldShardID, uint32_t fileIndex);
    void                WriteRecoveryMove(Buffer& src, Buffer& dst);
    void                DeleteGarbageShard(uint64_t shardID);
    void                RebuildShardTOC(uint64_t shardID);

    StorageDataPage*    CursorBegin(StorageCursor* cursor, ReadBuffer& key);

    void                CommitPhase1();
    void                CommitPhase2();
    void                CommitPhase3();
    void                CommitPhase4();

    FD                  tocFD;
    FD                  recoveryFD;
    Buffer              tocFilepath;
    Buffer              recoveryFilepath;
    Buffer              name;
    Buffer              path;
    Buffer              buffer;
    StorageIndexMap     shards;
    StorageDatabase*    database;
    
    friend class StorageCursor;
    friend class StorageDatabase;
};

#endif
