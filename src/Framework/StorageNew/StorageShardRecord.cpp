#include "StorageShardRecord.h"

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

bool StorageShard::RangeContains(ReadBuffer& key)
{
    int         f, l;
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
            return (cl < 0); //(key < lastKey);
    }
    else if (lastKey.GetLength() == 0)
    {
        return (cf <= 0); //(firstKey <= key);
    }
    else
        return (cf <= 0 && cl < 0); //(firstKey <= key < lastKey);
}

StorageChunk* StorageShard::GetWriteChunk()
{
    return chunks.Last();
}

void StorageShard::SetNewWriteChunk(StorageChunk* chunk)
{
    chunks.Append(chunk);
}
