#ifndef STORAGEBULKCURSOR_H
#define STORAGEBULKCURSOR_H

#include "System/Events/Callable.h"
#include "StorageFileKeyValue.h"
#include "StorageChunk.h"
#include "StorageShard.h"

class StorageEnvironment;
class StorageMemoChunk;
class StorageFileChunk;

/*
===============================================================================================

 StorageBulkCursor

===============================================================================================
*/

class StorageBulkCursor
{
public:
    StorageBulkCursor();
    ~StorageBulkCursor();

    void                    SetEnvironment(StorageEnvironment* env);
    void                    SetShard(uint64_t contextID_, uint64_t shardID);
    void                    SetOnBlockShard(Callable onBlockShard);
    
    StorageKeyValue*        First();
    StorageKeyValue*        Next(StorageKeyValue* it);

    void                    SetLast(bool last);
    ReadBuffer              GetNextKey();
    void                    SetNextKey(ReadBuffer key);
    void                    AppendKeyValue(StorageKeyValue* kv);
    void                    FinalizeKeyValues();

private:
    StorageKeyValue*        FromNextBunch(StorageChunk* chunk);

    bool                    blockShard;
    bool                    isLast;
    uint64_t                contextID;
    uint64_t                shardID;
    uint64_t                chunkID;
    Callable                onBlockShard;
    StorageShard*           shard;
    StorageEnvironment*     env;
    Buffer                  nextKey;
    StorageDataPage         dataPage;
};

#endif
