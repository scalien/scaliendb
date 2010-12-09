#include "StorageChunk.h"

StorageChunk::StorageChunk()
{
}

void StorageChunk::SetChunkID(uint64_t chunkID_)
{
    chunkID = chunkID;
}

void StorageChunk::SetUseBloomFilter(bool useBloomFilter_)
{
    useBloomFilter = useBloomFilter_;
}

uint64_t StorageChunk::GetChunkID(uint64_t chunkID)
{
    return chunkID;
}

bool StorageChunk::UseBloomFilter()
{
    return useBloomFilter;
}

bool StorageChunk::Get(ReadBuffer& firstKey, ReadBuffer& lastKey, ReadBuffer& key, ReadBuffer& value)
{
    if (state == ReadWrite)
        return tree->Get(firstKey, lastKey, key, value)
    else
        return file->Get(firstKey, lastKey, key, value);
}

bool StorageChunk::Set(ReadBuffer& key, ReadBuffer& value)
{
    if (state != ReadWrite)
        ASSERT_FAIL();
    
    return tree->Set(key, value);
}

bool StorageChunk::Delete(ReadBuffer& key)
{
    if (state != ReadWrite)
        ASSERT_FAIL();
    
    return tree->Delete(key);
}

void StorageChunk::RegisterLogCommand(uint64_t logSegmentID_, uint64_t logCommandID_)
{
    if (logSegmentID_ > logSegmentID)
    {
        logSegmentID = logSegmentID_;
        logCommandID = logCommandID_;
    }
    else if (logSegmentID_ == logSegmentID)
    {
        if (logCommandID_ > logCommandID)
            logCommandID = logCommandID_;
    }
}

uint64_t StorageChunk::GetSize()
{
    if (state == ReadWrite)
        return tree->GetSize();
    else
        return file->GetSize();
}

StorageChunk::ChunkState StorageChunk::GetState()
{
    return state;
}

void StorageChunk::TryFinalize()
{
    StorageChunkSerializer serializer;
    
    assert(state == ReadWrite);
        
    file = serializer.Serialize(this);
    if (file == NULL)
    {
        state = ReadWrite;
        return;
    }
    
    delete tree;
    tree = NULL;
    state = Serialized;
}

bool StorageChunk::IsFinalized()
{
    return (state != ReadWrite);
}
