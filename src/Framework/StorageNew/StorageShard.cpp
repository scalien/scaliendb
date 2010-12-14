#include "StorageShard.h"

inline bool LessThan(StorageChunk* a, StorageChunk* b)
{
    return (a->GetChunkID() < b->GetChunkID()); // TODO: this is old code
}

StorageShard::StorageShard()
{
    prev = next = this;
    contextID = 0;
    tableID = 0;
    shardID = 0;
    memoChunk = NULL;
}

void StorageShard::SetContextID(uint16_t contextID_)
{
    contextID = contextID_;
}

void StorageShard::SetTableID(uint64_t tableID_)
{
    tableID = tableID_;
}

void StorageShard::SetShardID(uint64_t shardID_)
{
    shardID = shardID_;
}

void StorageShard::SetFirstKey(ReadBuffer& firstKey_)
{
    firstKey.Write(firstKey_);
}

void StorageShard::SetLastKey(ReadBuffer& lastKey_)
{
    lastKey.Write(lastKey_);
}

void StorageShard::SetUseBloomFilter(bool useBloomFilter_)
{
    useBloomFilter = useBloomFilter_;
}

uint16_t StorageShard::GetContextID()
{
    return contextID;
}

uint64_t StorageShard::GetTableID()
{
    return tableID;
}

uint64_t StorageShard::GetShardID()
{
    return shardID;
}

ReadBuffer StorageShard::GetFirstKey()
{
    return ReadBuffer(firstKey);
}

ReadBuffer StorageShard::GetLastKey()
{
    return ReadBuffer(lastKey);
}

bool StorageShard::UseBloomFilter()
{
    return useBloomFilter;
}

bool StorageShard::RangeContains(ReadBuffer& key)
{
    int         cf, cl;
    ReadBuffer  firstKey, lastKey;

    firstKey = GetFirstKey();
    lastKey = GetLastKey();

    cf = ReadBuffer::Cmp(firstKey, key);
    cl = ReadBuffer::Cmp(key, lastKey);

    if (firstKey.GetLength() == 0)
    {
        if (lastKey.GetLength() == 0)
            return true;
        else
            return (cl < 0);        // (key < lastKey);
    }
    else if (lastKey.GetLength() == 0)
    {
        return (cf <= 0);           // (firstKey <= key);
    }
    else
        return (cf <= 0 && cl < 0); // (firstKey <= key < lastKey);
}

void StorageShard::PushMemoChunk(StorageMemoChunk* memoChunk_)
{
    if (memoChunk != NULL)
        chunks.Add(memoChunk);

    memoChunk = memoChunk_;
}

StorageMemoChunk* StorageShard::GetMemoChunk()
{
    return memoChunk;
}

StorageShard::ChunkList& StorageShard::GetChunks()
{
    return chunks;
}


void StorageShard::OnChunkSerialized(StorageMemoChunk* memoChunk, StorageFileChunk* fileChunk)
{
    StorageChunk* chunk;
    chunk = (StorageChunk*)memoChunk;

    chunks.Remove(chunk);
    chunks.Add(fileChunk);
}
