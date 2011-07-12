#include "StorageShard.h"

StorageShard::StorageShard()
{
    prev = next = this;
    trackID = 0;
    contextID = 0;
    tableID = 0;
    shardID = 0;
    memoChunk = NULL;
    recoveryLogSegmentID = 0;
    recoveryLogCommandID = 0;
    isLogStorage = false;
}

StorageShard::~StorageShard()
{
    StorageChunk**  itChunk;
    
    delete memoChunk;
    
    FOREACH (itChunk, chunks)
    {
        // StorageFileChunks are deleted in StorageEnvironment
        if ((*itChunk)->GetChunkState() != StorageChunk::Written &&
          (*itChunk)->GetChunkState() != StorageChunk::Unwritten)
        {
            delete *itChunk;
        }
    }
}

void StorageShard::SetTrackID(uint64_t trackID_)
{
    trackID = trackID_;
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

void StorageShard::SetLogSegmentID(uint64_t logSegmentID_)
{
    logSegmentID = logSegmentID_;
}

void StorageShard::SetLogCommandID(uint64_t logCommandID_)
{
    logCommandID = logCommandID_;
}

void StorageShard::SetFirstKey(ReadBuffer firstKey_)
{
    firstKey.Write(firstKey_);
}

void StorageShard::SetLastKey(ReadBuffer lastKey_)
{
    lastKey.Write(lastKey_);
}

void StorageShard::SetUseBloomFilter(bool useBloomFilter_)
{
    useBloomFilter = useBloomFilter_;
}

void StorageShard::SetLogStorage(bool isLogStorage_)
{
    isLogStorage = isLogStorage_;
}

uint64_t StorageShard::GetTrackID()
{
    return trackID;
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

uint64_t StorageShard::GetLogSegmentID()
{
    return logSegmentID;
}

uint32_t StorageShard::GetLogCommandID()
{
    return logCommandID;
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

bool StorageShard::IsLogStorage()
{
    return isLogStorage;
}

bool StorageShard::IsSplitable()
{
    StorageChunk**  itChunk;
    ReadBuffer      firstKey;
    ReadBuffer      lastKey;

    FOREACH (itChunk, GetChunks())
    {
        firstKey = (*itChunk)->GetFirstKey();
        lastKey = (*itChunk)->GetLastKey();
        
        if (firstKey.GetLength() > 0 && !RangeContains(firstKey))
            return false;

        if (lastKey.GetLength() > 0 && !RangeContains(lastKey))
            return false;
    }
    
    return true;
}

bool StorageShard::RangeContains(ReadBuffer key)
{
    ReadBuffer  firstKey, lastKey;

    firstKey = GetFirstKey();
    lastKey = GetLastKey();

    return ::RangeContains(firstKey, lastKey, key);
}

void StorageShard::PushMemoChunk(StorageMemoChunk* memoChunk_)
{
    if (memoChunk != NULL)
        chunks.Add(memoChunk);

    memoChunk = memoChunk_;
}

void StorageShard::PushChunk(StorageChunk* chunk)
{
    chunks.Add(chunk);
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
    chunk = (StorageChunk*) memoChunk;

    chunks.Remove(chunk);
    chunks.Add(fileChunk);
}


void StorageShard::GetMergeInputChunks(List<StorageFileChunk*>& inputChunks)
{
    uint64_t                oldSize;
    uint64_t                youngSize;
    uint64_t                totalSize;
    StorageFileChunk*       fileChunk;
    StorageChunk**          itChunk;
    StorageFileChunk**      itInputChunk;

    if (IsLogStorage())
        return;
    
    totalSize = 0;
    FOREACH (itChunk, chunks)
    {
        if ((*itChunk)->GetChunkState() != StorageChunk::Written)
            continue;
        fileChunk = (StorageFileChunk*) *itChunk;
        inputChunks.Append(fileChunk);
        totalSize += fileChunk->GetSize();
    }

    if (!IsSplitable())
        return;

    while (inputChunks.GetLength() >= 3)
    {
        itInputChunk = inputChunks.First();
        oldSize = (*itInputChunk)->GetSize();
        youngSize = totalSize - oldSize;
        if (oldSize > youngSize * 1.1)
        {
            inputChunks.Remove(inputChunks.First());
            totalSize -= oldSize;
        }
        else break;
    }
    
    if (inputChunks.GetLength() < 3)
        inputChunks.Clear();
}
