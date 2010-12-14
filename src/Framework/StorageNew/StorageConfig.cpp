#include "StorageConfig.h"

StorageConfig::StorageConfig()
{
    chunkSize = STORAGE_DEFAULT_CHUNKSIZE;
    logSegmentSize = STORAGE_DEFAULT_LOGSEGMENTSIZE;
    fileChunkCacheSize = STORAGE_DEFAULT_FILECHUNK_CACHESIZE;
}
