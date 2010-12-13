#include "StorageMemoChunk.h"
#include "StorageChunkSerializer.h"
#include "StorageChunkWriter.h"

static int KeyCmp(const ReadBuffer a, const ReadBuffer b)
{
    return ReadBuffer::Cmp(a, b);
}

static const ReadBuffer Key(StorageMemoKeyValue* kv)
{
    return kv->GetKey();
}

StorageMemoChunk::StorageMemoChunk()
{
    serialized = false;
    chunkID = 0;
    logSegmentID = 0;
    logCommandID = 0;
    useBloomFilter = false;
    size = 0;
}

StorageMemoChunk::~StorageMemoChunk()
{
    assert(fileChunk == NULL);
    keyValues.DeleteTree();
}

StorageChunk::ChunkState StorageMemoChunk::GetChunkState()
{
    if (serialized)
        return StorageChunk::Serialized;
    else
        return StorageChunk::Tree;
}

void StorageMemoChunk::SetChunkID(uint64_t chunkID_)
{
    chunkID = chunkID_;
}

void StorageMemoChunk::SetUseBloomFilter(bool useBloomFilter_)
{
    useBloomFilter = useBloomFilter_;
}

uint64_t StorageMemoChunk::GetChunkID()
{
    return chunkID;
}

bool StorageMemoChunk::UseBloomFilter()
{
    return useBloomFilter;
}

StorageKeyValue* StorageMemoChunk::Get(ReadBuffer& key)
{
    StorageMemoKeyValue* it;

    int cmpres;
    
    if (keyValues.GetCount() == 0)
        return NULL;
        
    it = keyValues.Locate<ReadBuffer&>(key, cmpres);
    if (cmpres != 0)
        return NULL;

    return it;
}

bool StorageMemoChunk::Set(ReadBuffer& key, ReadBuffer& value)
{
    int                     cmpres;
    StorageMemoKeyValue*    it;
    
    if (keyValues.GetCount() != 0)
    {
        it = keyValues.Locate<ReadBuffer&>(key, cmpres);
        if (cmpres == 0)
        {
            size -= it->GetLength();
            it->Set(key, value);
            size += it->GetLength();
            return true;
        }
    }   

    it = new StorageMemoKeyValue;
    it->Set(key, value);
    keyValues.Insert(it);
    size += it->GetLength();
    
    return true;
}

bool StorageMemoChunk::Delete(ReadBuffer& key)
{
    int                     cmpres;
    StorageMemoKeyValue*    it;
    
    if (keyValues.GetCount() != 0)
    {
        it = keyValues.Locate<ReadBuffer&>(key, cmpres);
        if (cmpres == 0)
        {
            size -= it->GetLength();
            it->Delete(key);
            size += it->GetLength();
            return true;
        }
    }   

    it = new StorageMemoKeyValue;
    it->Delete(key);
    keyValues.Insert(it);
    size += it->GetLength();
    
    return true;
}

void StorageMemoChunk::RegisterLogCommand(uint64_t logSegmentID_, uint32_t logCommandID_)
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

uint64_t StorageMemoChunk::GetLogSegmentID()
{
    return logSegmentID;
}

uint32_t StorageMemoChunk::GetLogCommandID()
{
    return logCommandID;
}

uint64_t StorageMemoChunk::GetSize()
{
    return size;
}

StorageFileChunk* StorageMemoChunk::RemoveFileChunk()
{
    StorageFileChunk* ret;
    ret = fileChunk;
    fileChunk = NULL;
    return ret;
}
