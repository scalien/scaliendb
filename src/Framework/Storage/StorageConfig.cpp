#include "StorageConfig.h"

void StorageConfig::SetChunkSize(uint64_t chunkSize_)
{
    chunkSize = chunkSize_;
}

void StorageConfig::SetLogSegmentSize(uint64_t logSegmentSize_)
{
    logSegmentSize = logSegmentSize_;
}

void StorageConfig::SetFileChunkCacheSize(uint64_t fileChunkCacheSize_)
{
    fileChunkCacheSize = fileChunkCacheSize_;
}

void StorageConfig::SetMemoChunkCacheSize(uint64_t memoChunkCacheSize_)
{
    memoChunkCacheSize = memoChunkCacheSize_;
}

void StorageConfig::SetLogSize(uint64_t logSize_)
{
    numLogSegments = logSize_ / logSegmentSize;
    if (numLogSegments == 0)
        numLogSegments = 1;
}

void StorageConfig::SetMergeBufferSize(uint64_t mergeBufferSize_)
{
    mergeBufferSize = mergeBufferSize_;
}

void StorageConfig::SetMergeYieldFactor(uint64_t mergeYieldFactor_)
{
    mergeYieldFactor = mergeYieldFactor_;
}

void StorageConfig::SetSyncGranularity(uint64_t syncGranularity_)
{
    syncGranularity = syncGranularity_;
}

void StorageConfig::SetWriteGranularity(uint64_t writeGranularity_)
{
    writeGranularity = writeGranularity_;
}

void StorageConfig::SetReplicatedLogSize(uint64_t replicatedLogSize)
{
    numLogSegmentFileChunks = replicatedLogSize / chunkSize;
    if (numLogSegmentFileChunks == 0)
        numLogSegmentFileChunks = 1;
}

void StorageConfig::SetAbortWaitingListsNum(uint64_t abortWaitingListsNum_)
{
    abortWaitingListsNum = abortWaitingListsNum_;
}

void StorageConfig::SetListDataPageCacheSize(uint64_t listDataPageCacheSize_)
{
    listDataPageCacheSize = listDataPageCacheSize_;
}

void StorageConfig::SetMaxChunkPerShard(unsigned maxChunkPerShard_)
{
    maxChunkPerShard = maxChunkPerShard_;
}

uint64_t StorageConfig::GetChunkSize()
{
    return chunkSize;
}

uint64_t StorageConfig::GetLogSegmentSize()
{
    return logSegmentSize;
}

uint64_t StorageConfig::GetFileChunkCacheSize()
{
    return fileChunkCacheSize;
}

uint64_t StorageConfig::GetMemoChunkCacheSize()
{
    return memoChunkCacheSize;
}

uint64_t StorageConfig::GetNumLogSegments()
{
    return numLogSegments;
}

uint64_t StorageConfig::GetMergeBufferSize()
{
    return mergeBufferSize;
}

uint64_t StorageConfig::GetMergeYieldFactor()
{
    return mergeYieldFactor;
}

uint64_t StorageConfig::GetSyncGranularity()
{
    return syncGranularity;
}

uint64_t StorageConfig::GetWriteGranularity()
{
    return writeGranularity;
}

uint64_t StorageConfig::GetNumLogSegmentFileChunks()
{
    return numLogSegmentFileChunks;
}

uint64_t StorageConfig::GetAbortWaitingListsNum()
{
    return abortWaitingListsNum;
}

uint64_t StorageConfig::GetListDataPageCacheSize()
{
    return listDataPageCacheSize;
}

unsigned StorageConfig::GetMaxChunkPerShard()
{
    return maxChunkPerShard;
}
