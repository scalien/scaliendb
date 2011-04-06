#ifndef STORAGEBULKCURSOR_H
#define STORAGEBULKCURSOR_H

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
    
    StorageKeyValue*        First();
    StorageKeyValue*        Next(StorageKeyValue* it);

    void                    SetLast(bool last);
    ReadBuffer              GetNextKey();
    void                    SetNextKey(ReadBuffer key);
    void                    AppendKeyValue(StorageKeyValue* kv);
    void                    FinalizeKeyValues();

    void                    SetEnvironment(StorageEnvironment* env);
    void                    SetShard(uint64_t contextID_, uint64_t shardID);

private:
    StorageKeyValue*        FromNextBunch(StorageChunk* chunk);

    bool                    isLast;
    uint64_t                contextID;
    uint64_t                shardID;
    uint64_t                chunkID;
    StorageShard*           shard;
    StorageEnvironment*     env;
    Buffer                  nextKey;
    StorageDataPage         dataPage;
};

#endif
