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

 StorageCursorBunch

===============================================================================================
*/

class StorageCursorBunch
{
    friend class StorageMemoChunk;
    friend class StorageFileChunk;
    typedef InTreeMap<StorageFileKeyValue> KeyValueTree;

public:
    StorageKeyValue*        First();
    StorageKeyValue*        Next(StorageKeyValue* it);
    
    ReadBuffer              GetNextKey();
    bool                    IsLast();
    void                    Reset();
private:
    
    KeyValueTree            keyValues;
    Buffer                  buffer;
    bool                    isLast;
    Buffer                  nextKey;
};

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

private:
    StorageKeyValue*        FromNextBunch(StorageChunk* chunk);
    void                    SetEnvironment(StorageEnvironment* env);
    void                    SetShard(StorageShard* shard);

    StorageCursorBunch      bunch;

    uint64_t                chunkID;
    StorageShard*           shard;
    StorageEnvironment*     env;
};

#endif
