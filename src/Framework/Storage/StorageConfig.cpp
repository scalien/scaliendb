#include "StorageConfig.h"
#include "System/Config.h"

void StorageConfig::Init()
{
    uint64_t size;

    chunkSize = (uint64_t) configFile.GetInt64Value("database.chunkSize", 
     STORAGE_DEFAULT_CHUNKSIZE);

    logSegmentSize = (uint64_t) configFile.GetInt64Value("database.logSegmentSize", 
     STORAGE_DEFAULT_LOGSEGMENTSIZE);

    fileChunkCacheSize = (uint64_t) configFile.GetInt64Value("database.fileChunkCacheSize", 
     STORAGE_DEFAULT_FILECHUNK_CACHESIZE);

    memoChunkCacheSize = (uint64_t) configFile.GetInt64Value("database.memoChunkCacheSize", 
     STORAGE_DEFAULT_MEMOCHUNK_CACHESIZE);

    size = (uint64_t) configFile.GetInt64Value("database.logSize", 
     STORAGE_DEFAULT_LOG_SIZE);
    numUnbackedLogSegments = size / logSegmentSize;

    mergeBufferSize = (uint64_t) configFile.GetInt64Value("database.mergeBufferSize", 
     STORAGE_DEFAULT_MERGEBUFFER_SIZE);

    syncGranularity = (uint64_t) configFile.GetInt64Value("database.syncGranularity", 
     STORAGE_DEFAULT_SYNC_GRANULARITY);

    size = configFile.GetInt64Value("database.replicatedLogSize", STORAGE_DEFAULT_REPLICATEDLOG_SIZE);
    numLogSegmentFileChunks = size / chunkSize;
}
