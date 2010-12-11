#include "StorageChunk.h"

StorageChunk::StorageChunk()
{
}

void StorageChunk::SetFilename(Buffer& filename_)
{
    filename.Write(filename_);
    filename.NullTerminate();
}

void StorageChunk::SetChunkID(uint64_t chunkID_)
{
    chunkID = chunkID;
}

void StorageChunk::SetUseBloomFilter(bool useBloomFilter_)
{
    useBloomFilter = useBloomFilter_;
}

uint64_t StorageChunk::GetChunkID()
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
        return keyValues->Get(firstKey, lastKey, key, value);
    else
        return keyValues->Get(firstKey, lastKey, key, value);
}

bool StorageChunk::Set(ReadBuffer& key, ReadBuffer& value)
{
    if (state != ReadWrite)
        ASSERT_FAIL();
    
    return keyValues->Set(key, value);
}

bool StorageChunk::Delete(ReadBuffer& key)
{
    if (state != ReadWrite)
        ASSERT_FAIL();
    
    return keyValues->Delete(key);
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

uint64_t StorageChunk::GetLogSegmentID()
{
    return logSegmentID;
}

uint64_t StorageChunk::GetLogCommandID()
{
    return logCommandID;
}

uint64_t StorageChunk::GetNumKeys()
{
    if (state == ReadWrite)
        return keyValues->GetNumKeys();
    else
        return file->GetNumKeys();    
}

StorageChunk::KeyValueTree& StorageChunk::GetKeyValueTree()
{
    assert(tree != NULL);

    return *tree;
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
    
    delete keyValues;
    keyValues = NULL;
    state = Serialized;
}

bool StorageChunk::IsFinalized()
{
    return (state != ReadWrite);
}

void StorageChunk::WriteFile()
{
}
