#include "StorageShard.h"

static inline bool LessThan(ReadBuffer& a, ReadBuffer& b)
{
    return ReadBuffer::Cmp(a, b) < 0 ? true : false;
}

static inline bool operator==(const ReadBuffer& a, const ReadBuffer& b)
{
    return ReadBuffer::Cmp(a, b) == 0 ? true : false;
}

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
    storageType = STORAGE_SHARD_TYPE_STANDARD;
    
    InvalidateCachedValues();
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

    // This happens on shard splitting
    InvalidateCachedValues();
}

void StorageShard::SetLastKey(ReadBuffer lastKey_)
{
    lastKey.Write(lastKey_);

    // This happens on shard splitting
    InvalidateCachedValues();
}

void StorageShard::SetUseBloomFilter(bool useBloomFilter_)
{
    useBloomFilter = useBloomFilter_;
}

void StorageShard::SetStorageType(char storageType_)
{
    storageType = storageType_;
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

ReadBuffer StorageShard::GetMidpoint()
{
    ReadBuffer  midpoint;

    if (IsPrecomputeNecessary())
        PrecomputeCachedValues();

    midpoint.Wrap(cachedMidpoint);
    return midpoint;
}

uint64_t StorageShard::GetSize()
{
    if (IsPrecomputeNecessary())
        PrecomputeCachedValues();

    return cachedSize;
}

bool StorageShard::UseBloomFilter()
{
    return useBloomFilter;
}

char StorageShard::GetStorageType()
{
    return storageType;
}

bool StorageShard::IsSplitable()
{
    if (IsPrecomputeNecessary())
        PrecomputeCachedValues();

    return cachedSplitable;
}

bool StorageShard::IsChunkSplitable(StorageChunk* chunk)
{
    ReadBuffer      firstKey;
    ReadBuffer      lastKey;

    firstKey = chunk->GetFirstKey();
    lastKey = chunk->GetLastKey();

    if (ReadBuffer::Cmp(chunk->GetMidpoint(), "ScalienDB-Midpoint") == 0)
        return false;
        
    if (firstKey.GetLength() > 0 && !RangeContains(firstKey))
        return false;

    if (lastKey.GetLength() > 0 && !RangeContains(lastKey))
        return false;

    return true;
}

bool StorageShard::IsBackingLogSegment(uint64_t checkTrackID, uint64_t checkLogSegmentID)
{
    StorageChunk** chunk;

    if (trackID != checkTrackID)
        return false;

    if (storageType == STORAGE_SHARD_TYPE_LOG)
        return false; // log storage shards never hinder log segment archival

    if (storageType == STORAGE_SHARD_TYPE_DUMP)
    {
        if (checkLogSegmentID + 1 < memoChunk->GetMaxLogSegmentID())
            return false;
            // dump storage shard has been written to a higher numbered log segment, do not hinder archival
    }

    if (memoChunk->GetSize() > 0)
    if (memoChunk->GetMinLogSegmentID() > 0)
    if (memoChunk->GetMinLogSegmentID() <= checkLogSegmentID)
    if (checkLogSegmentID <= memoChunk->GetMaxLogSegmentID())
        return true;

    FOREACH (chunk, chunks)
    {
        if ((*chunk)->GetChunkState() <= StorageChunk::Unwritten)
        if ((*chunk)->GetMinLogSegmentID() <= checkLogSegmentID)
        if (checkLogSegmentID <= (*chunk)->GetMaxLogSegmentID())
            return true;
    }

    return false;
}

bool StorageShard::RangeContains(ReadBuffer key)
{
    return ::RangeContains(ReadBuffer(firstKey), ReadBuffer(lastKey), key);
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

bool StorageShard::IsMergeableType()
{
    if (GetStorageType() == STORAGE_SHARD_TYPE_STANDARD)
        return true;

    return false;
}

bool StorageShard::IsSplitMergeCandidate()
{
    unsigned        count;
    StorageChunk**  itChunk;

    if (!IsMergeableType())
        return false;

    // if it's a splitable data (non-paxos) shard, then we don't need to merge it
    if (IsSplitable() && tableID > 0)
        return false;
    
    count = 0;
    FOREACH (itChunk, chunks)
    {
        if ((*itChunk)->GetChunkState() == StorageChunk::Written)
            count++;
        else
            return false;   // don't merge unwritten shards
    }

    if (count > 1)
        return true;
    else
        return false;
}

bool StorageShard::IsFragmentedMergeCandidate(unsigned maxChunkPerShard)
{
    unsigned        count;
    StorageChunk**  itChunk;

    if (!IsMergeableType())
        return false;
    
    count = 0;
    FOREACH (itChunk, chunks)
    {
        if ((*itChunk)->GetChunkState() == StorageChunk::Written)
            count++;
        else
            return false;   // don't merge unwritten shards
    }

    if (count > maxChunkPerShard)
        return true;
    else
        return false;
}

void StorageShard::GetMergeInputChunks(List<StorageFileChunk*>& inputChunks)
{
    StorageFileChunk*   fileChunk;
    StorageChunk**      itChunk;
    
    FOREACH (itChunk, chunks)
    {
        if ((*itChunk)->GetChunkState() == StorageChunk::Written)
        {
            fileChunk = (StorageFileChunk*) *itChunk;
            inputChunks.Append(fileChunk);
        }
        else
            ASSERT_FAIL();
    }

    ASSERT(inputChunks.GetLength() > 1);
}

void StorageShard::InvalidateCachedValues()
{
    cachedMidpoint.Reset();
    cachedSize = 0;
    cachedNumChunks = 0;
    cachedSplitable = false;
}

void StorageShard::PrecomputeCachedValues()
{
    unsigned                i;
    StorageChunk**          itChunk;
    ReadBuffer              midpoint;
    SortedList<ReadBuffer>  midpoints;
    ReadBuffer*             itMidpoint;
    uint64_t                size;
    uint64_t                chunkSize;
    bool                    splitable;

    InvalidateCachedValues();

    midpoint = memoChunk->GetMidpoint();
    if (midpoint.GetLength() > 0)
        midpoints.Add(midpoint);

    size = 0;
    splitable = true;
    FOREACH (itChunk, chunks)
    {
        if ((*itChunk)->GetChunkState() == StorageChunk::Written)
        {
            chunkSize = 0;
            midpoint = ((StorageFileChunk*)(*itChunk))->GetPartialMidpointAndSize(firstKey, lastKey, chunkSize);
            size += chunkSize;
        }
        else
        {
            midpoint = (*itChunk)->GetMidpoint();
            size += (*itChunk)->GetSize();
        }

        splitable &= IsChunkSplitable(*itChunk);

        if (midpoint.GetLength() > 0)
            midpoints.Add(midpoint);
    }

    i = 0;
    FOREACH (itMidpoint, midpoints)
    {
        if (i >= (midpoints.GetLength() / 2))
        {
            cachedMidpoint.Write(midpoint);
            cachedSize = size;
            cachedNumChunks = chunks.GetLength();
            cachedSplitable = splitable;
            return;
        }
        i++;
    }
}

bool StorageShard::IsPrecomputeNecessary()
{
    // If the number of chunks did not change and the size of the memoChunk
    // is not significant compared to the size of the shard, then we can safely assume
    // the shard is not changed.
    if (chunks.GetLength() == cachedNumChunks && cachedSize > memoChunk->GetSize() * 2)
        return false;

    return true;
}