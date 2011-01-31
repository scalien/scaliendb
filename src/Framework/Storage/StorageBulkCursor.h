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
    void                    SetShard(StorageShard* shard);

private:
    StorageKeyValue*        FromNextBunch(StorageChunk* chunk);

    StorageDataPage         dataPage;
    uint64_t                chunkID;
    StorageShard*           shard;
    StorageEnvironment*     env;
    bool                    isLast;
    Buffer                  nextKey;
};

#endif
