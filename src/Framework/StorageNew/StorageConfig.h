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

class StorageConfig
{
public:
    StorageConfig();

    uint32_t        chunkSize;
    uint32_t        logSegmentSize;
    uint64_t        fileChunkCacheSize;
};

#endif
