#include "StorageMemoChunk.h"
#include "StorageChunkSerializer.h"
#include "StorageChunkWriter.h"
#include "StorageBulkCursor.h"

static inline int KeyCmp(const ReadBuffer& a, const ReadBuffer& b)
{
    return ReadBuffer::Cmp(a, b);
}

static inline const ReadBuffer Key(StorageMemoKeyValue* kv)
{
    return kv->GetKey();
}

static inline const ReadBuffer Key(StorageFileKeyValue* kv)
{
    return kv->GetKey();
}

StorageMemoChunk::StorageMemoChunk()
{
    serialized = false;
    chunkID = 0;
    minLogSegmentID = 0;
    maxLogSegmentID = 0;
    maxLogCommandID = 0;
    useBloomFilter = false;
    size = 0;
    fileChunk = NULL;
}

StorageMemoChunk::~StorageMemoChunk()
{
    // TODO: see issue with StorageEnvironment::Close()
    //assert(fileChunk == NULL);
    keyValues.DeleteTree();
}

StorageChunk::ChunkState StorageMemoChunk::GetChunkState()
{
    if (serialized)
        return StorageChunk::Serialized;
    else
        return StorageChunk::Tree;
}

void StorageMemoChunk::NextBunch(StorageCursorBunch& bunch, StorageShard* shard)
{
    bool                    first;
    int                     cmpres;
    uint32_t                pos;
    ReadBuffer              nextKey, key, value;
    StorageMemoKeyValue*    it;
    StorageFileKeyValue*    kv;
    
    if (keyValues.GetCount() == 0)
    {
        bunch.isLast = true;
        return;
    }
    
    nextKey = bunch.GetNextKey();
    it = keyValues.Locate<ReadBuffer&>(nextKey, cmpres);
    
    bunch.buffer.Allocate(STORAGE_MEMO_BUNCH_GRAN);
    bunch.buffer.SetLength(0);
    first = true;
    while (it)
    {
        if (!shard->RangeContains(it->GetKey()))
        {
            it = keyValues.Next(it);
            continue;
        }
        
        kv = new StorageFileKeyValue;
        if (!first)
        {
            if (bunch.buffer.GetLength() +
             it->GetKey().GetLength() + it->GetValue().GetLength() > bunch.buffer.GetSize())
                break;
        }
        bunch.buffer.Append(it->GetKey());
        if (it->GetType() == STORAGE_KEYVALUE_TYPE_SET)
            bunch.buffer.Append(it->GetValue());
        pos = bunch.buffer.GetLength() - (it->GetKey().GetLength() + it->GetValue().GetLength());
        key.Wrap(bunch.buffer.GetBuffer() + pos, it->GetKey().GetLength());
        if (it->GetType() == STORAGE_KEYVALUE_TYPE_SET)
        {
            pos = bunch.buffer.GetLength() - it->GetValue().GetLength();
            value.Wrap(bunch.buffer.GetBuffer() + pos, it->GetValue().GetLength());
            kv->Set(key, value);
        }
        else
        {
            kv->Delete(key);
        }
        bunch.keyValues.Insert(kv);
        first = false;
        it = keyValues.Next(it);
    }
    
    if (!it)
    {
        bunch.isLast = true;
    }
    else
    {
        bunch.nextKey.Write(it->GetKey());
        bunch.isLast = false;
    }
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
    int                     cmpres;
    StorageMemoKeyValue*    it;

    
    if (keyValues.GetCount() == 0)
        return NULL;
        
    it = keyValues.Locate<ReadBuffer&>(key, cmpres);
    if (cmpres != 0)
        return NULL;

    return it;
}

bool StorageMemoChunk::Set(ReadBuffer key, ReadBuffer value)
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

bool StorageMemoChunk::Delete(ReadBuffer key)
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
    if (minLogSegmentID == 0)
        minLogSegmentID = logSegmentID_;
    
    if (logSegmentID_ > maxLogSegmentID)
    {
        maxLogSegmentID = logSegmentID_;
        maxLogCommandID = logCommandID_;
    }
    else if (logSegmentID_ == maxLogSegmentID)
    {
        if (logCommandID_ > maxLogCommandID)
            maxLogCommandID = logCommandID_;
    }
}

uint64_t StorageMemoChunk::GetMinLogSegmentID()
{
    return minLogSegmentID;
}

uint64_t StorageMemoChunk::GetMaxLogSegmentID()
{
    return maxLogSegmentID;
}

uint32_t StorageMemoChunk::GetMaxLogCommandID()
{
    return maxLogCommandID;
}

ReadBuffer StorageMemoChunk::GetFirstKey()
{
    if (keyValues.First())
        return keyValues.First()->GetKey();
    else
        return ReadBuffer();
}

ReadBuffer StorageMemoChunk::GetLastKey()
{
    if (keyValues.Last())
        return keyValues.Last()->GetKey();
    else
        return ReadBuffer();
}

uint64_t StorageMemoChunk::GetSize()
{
    return size;
}

ReadBuffer StorageMemoChunk::GetMidpoint()
{
    if (keyValues.Mid())
        return keyValues.Mid()->GetKey();
    else
        return ReadBuffer();
}

StorageFileChunk* StorageMemoChunk::RemoveFileChunk()
{
    StorageFileChunk*   ret;
    
    ret = fileChunk;
    fileChunk = NULL;
    return ret;
}
