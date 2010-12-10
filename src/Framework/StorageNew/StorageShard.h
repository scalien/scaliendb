#ifndef STORAGESHARD_H
#define STORAGESHARD_H

/*
===============================================================================================

 StorageShard

===============================================================================================
*/

class StorageShard
{
    typedef SortedList<StorageChunk*> ChunkList;

public:    
    uint64_t        GetTableID();
    uint64_t        GetShardID();

    ReadBuffer      GetFirstKey();
    ReadBuffer      GetLastKey();
    bool            RangeContains(ReadBuffer& key);

    StorageChunk*   GetWriteChunk();
    void            OnChunkFinalized(StorageChunk* newWriteChunk);

private:
    uint64_t        shardID;
    uint64_t        tableID;

    ChunkList       chunks; // contains all active chunks, in order, first is the write chunk

    Buffer          firstKey;
    Buffer          lastKey;
};

#endif
