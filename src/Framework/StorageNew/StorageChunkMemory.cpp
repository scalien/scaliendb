#include "StorageChunk.h"
#include "StorageChunkSerializer.h"
#include "StorageChunkWriter.h"

StorageChunk::StorageChunk()
{
    chunkID = 0;
    logSegmentID = 0;
    logCommandID = 0;
    useBloomFilter = false;
    
    // in memory:
    state = ReadWrite;
    numShards = 1;

    keyValues = 0;
    file = 0;
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
        return file->Get(firstKey, lastKey, key, value);
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

StorageChunk::KeyValueTree& StorageChunk::GetKeyValueTree()
{
    assert(keyValues != NULL);

    return *keyValues;
}

uint64_t StorageChunk::GetSize()
{
    if (state == ReadWrite)
        return keyValues->GetSize();
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
    StorageChunkWriter writer;

    assert(state == Serialized);

    if (!writer.Write(filename.GetBuffer(), file))
        return;
    
    state = Written;

    // TODO: mark file/pages as evictable
}
