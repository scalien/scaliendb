#include "StorageChunkMemory.h"
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

StorageChunkMemory::StorageChunkMemory()
{
    chunkID = 0;
    logSegmentID = 0;
    logCommandID = 0;
    useBloomFilter = false;
    size = 0;
}

void StorageChunkMemory::SetChunkID(uint64_t chunkID_)
{
    chunkID = chunkID;
}

void StorageChunkMemory::SetUseBloomFilter(bool useBloomFilter_)
{
    useBloomFilter = useBloomFilter_;
}

uint64_t StorageChunkMemory::GetChunkID()
{
    return chunkID;
}

bool StorageChunkMemory::UseBloomFilter()
{
    return useBloomFilter;
}

bool StorageChunkMemory::Get(ReadBuffer& key, ReadBuffer& value)
{
    StorageMemoKeyValue* it;

    int cmpres;
    
    if (keyValues.GetCount() == 0)
        return false;
        
    it = keyValues.Locate<ReadBuffer&>(key, cmpres);
    if (cmpres != 0)
        return false;

    value = it->GetValue();
    return true;
}

bool StorageChunkMemory::Set(ReadBuffer& key, ReadBuffer& value)
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

bool StorageChunkMemory::Delete(ReadBuffer& key)
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

void StorageChunkMemory::RegisterLogCommand(uint64_t logSegmentID_, uint32_t logCommandID_)
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

uint64_t StorageChunkMemory::GetLogSegmentID()
{
    return logSegmentID;
}

uint32_t StorageChunkMemory::GetLogCommandID()
{
    return logCommandID;
}

uint64_t StorageChunkMemory::GetSize()
{
    return size;
}

//void StorageChunkMemory::TryFinalize()
//{
//    StorageChunkSerializer serializer;
//    
//    assert(state == ReadWrite);
//        
//    file = serializer.Serialize(this);
//    if (file == NULL)
//    {
//        state = ReadWrite;
//        return;
//    }
//    
//    delete keyValues;
//    keyValues = NULL;
//    state = Serialized;
//}
//
//bool StorageChunkMemory::IsFinalized()
//{
//    return (state != ReadWrite);
//}
//
//void StorageChunkMemory::WriteFile()
//{
//    StorageChunkWriter writer;
//
//    assert(state == Serialized);
//
//    if (!writer.Write(filename.GetBuffer(), file))
//        return;
//    
//    state = Written;
//
//    // TODO: mark file/pages as evictable
//}
