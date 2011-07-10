#include "StorageMemoChunk.h"
#include "StorageChunkSerializer.h"
#include "StorageChunkWriter.h"
#include "StorageBulkCursor.h"
#include "StorageEnvironment.h"
#include "StorageAsyncGet.h"

static inline int KeyCmp(const ReadBuffer& a, const ReadBuffer& b)
{
    return ReadBuffer::Cmp(a, b);
}

static inline const ReadBuffer Key(const StorageMemoKeyValue* kv)
{
    return kv->GetKey();
}

static inline const ReadBuffer Key(const StorageFileKeyValue* kv)
{
    return kv->GetKey();
}

StorageMemoChunk::StorageMemoChunk(uint64_t chunkID_, bool useBloomFilter_)
{
    chunkID = chunkID_;
    useBloomFilter = useBloomFilter_;

    serialized = false;
    minLogSegmentID = 0;
    maxLogSegmentID = 0;
    maxLogCommandID = 0;
    size = 0;
    fileChunk = NULL;
    deleted = false;
}

StorageMemoChunk::~StorageMemoChunk()
{
    StorageMemoKeyValueBlock*   block;
    //keyValues.DeleteTree();

    while (keyValueBlocks.GetLength() > 0)
    {
        block = keyValueBlocks.Dequeue();
        delete block;
    }

    if (fileChunk != NULL)
        delete fileChunk;
}

StorageChunk::ChunkState StorageMemoChunk::GetChunkState()
{
    if (serialized)
        return StorageChunk::Serialized;
    else
        return StorageChunk::Tree;
}

void StorageMemoChunk::NextBunch(StorageBulkCursor& cursor, StorageShard* shard)
{
    bool                    first;
    int                     cmpres;
    ReadBuffer              nextKey, key, value;
    StorageMemoKeyValue*    it;
    uint32_t                total;
    
    if (keyValues.GetCount() == 0)
    {
        cursor.SetLast(true);
        return;
    }
    
    nextKey = cursor.GetNextKey();
    it = keyValues.Locate(nextKey, cmpres);
    
    total = 0;
    first = true;
    while (it)
    {
        if (!shard->RangeContains(it->GetKey()))
        {
            it = keyValues.Next(it);
            continue;
        }

        if (!first && total + it->GetKey().GetLength() + it->GetValue().GetLength() > STORAGE_MEMO_BUNCH_GRAN)
            break;
        
        total += it->GetKey().GetLength() + it->GetValue().GetLength();
        cursor.AppendKeyValue(it);
        
        it = keyValues.Next(it);
        first = false;
    }
    cursor.FinalizeKeyValues();
    
    if (!it)
    {
        cursor.SetLast(true);
    }
    else
    {
        cursor.SetNextKey(it->GetKey());
        cursor.SetLast(false);
    }
}

//void StorageMemoChunk::SetChunkID(uint64_t chunkID_)
//{
//    chunkID = chunkID_;
//}
//
//void StorageMemoChunk::SetUseBloomFilter(bool useBloomFilter_)
//{
//    useBloomFilter = useBloomFilter_;
//}

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
        
    it = keyValues.Locate<const ReadBuffer&>(key, cmpres);
    if (cmpres != 0)
        return NULL;

    return it;
}

void StorageMemoChunk::AsyncGet(StorageAsyncGet* asyncGet)
{
    StorageKeyValue*    kv;
    
    Log_Debug("StorageMemoChunk::AsyncGet");
    
    kv = Get(asyncGet->key);
    if (kv == NULL || kv->GetType() == STORAGE_KEYVALUE_TYPE_DELETE)
    {
        asyncGet->ret = false;
        asyncGet->completed = true;
        return;
    }

    asyncGet->value = kv->GetValue();
    asyncGet->ret = true;
    asyncGet->completed = true;
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

    //it = new StorageMemoKeyValue;
    it = NewStorageMemoKeyValue();
    it->Set(key, value);
    keyValues.Insert<const ReadBuffer>(it);
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

    //it = new StorageMemoKeyValue;
    it = NewStorageMemoKeyValue();
    it->Delete(key);
    keyValues.Insert<const ReadBuffer>(it);
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

bool StorageMemoChunk::IsEmpty()
{
    return (size == 0);
}

StorageFileChunk* StorageMemoChunk::RemoveFileChunk()
{
    StorageFileChunk*   ret;
    
    ret = fileChunk;
    fileChunk = NULL;
    return ret;
}

void StorageMemoChunk::RemoveFirst()
{
    StorageMemoKeyValueBlock*   block;
    StorageMemoKeyValue*        first;

    first = keyValues.First();
    size -= first->GetLength();
    keyValues.Remove(first);
    
    // TODO: memleak!!!
    
    //delete first;

    // assuming first is on the first block
    block = keyValueBlocks.First();
    ASSERT(first >= block->keyValues && first < (block->keyValues + STORAGE_BLOCK_NUM_KEY_VALUE));
    block->last++;
    if (block->last == STORAGE_BLOCK_NUM_KEY_VALUE)
    {
        keyValueBlocks.Dequeue();
        delete block;
        size -= sizeof(StorageMemoKeyValueBlock);
    }
}

StorageMemoKeyValue* StorageMemoChunk::NewStorageMemoKeyValue()
{
    StorageMemoKeyValueBlock*   block;
    StorageMemoKeyValue*        keyValue;
    bool                        isNewBlockNeeded;

    isNewBlockNeeded = false;
    
    if (keyValueBlocks.GetLength() == 0)
        isNewBlockNeeded = true;
    else
    {
        block = keyValueBlocks.Last();
        if (block->first == STORAGE_BLOCK_NUM_KEY_VALUE)
            isNewBlockNeeded = true;
    }

    if (isNewBlockNeeded)
    {
        block = new StorageMemoKeyValueBlock;
        block->next = block;
        block->first = 0;
        block->last = 0;
        keyValueBlocks.Enqueue(block);
        size += sizeof(StorageMemoKeyValueBlock);
    }

    keyValue = &block->keyValues[block->first];
    block->first++;

    return keyValue;
}
