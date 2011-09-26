#ifndef STORAGECONFIG_H
#define STORAGECONFIG_H

#include "System/Common.h"

/*
===============================================================================================

 StorageJob

===============================================================================================
*/

#define STORAGE_DEFAULT_CHUNKSIZE               64*MiB
#define STORAGE_DEFAULT_LOGSEGMENTSIZE          32*MiB
#define STORAGE_DEFAULT_FILECHUNK_CACHESIZE     256*MiB
#define STORAGE_DEFAULT_MERGEBUFFER_SIZE        10*MiB
#define STORAGE_DEFAULT_SYNC_GRANULARITY        16*MiB
#define STORAGE_DEFAULT_REPLICATEDLOG_SIZE      10*GiB

class StorageConfig
{
public:
    void            Init();

    uint64_t        chunkSize;
    uint64_t        logSegmentSize;
    uint64_t        fileChunkCacheSize;
    uint64_t        mergeBufferSize;
    uint64_t        syncGranularity;
    uint64_t        numLogSegmentFileChunks;
};

#endif
