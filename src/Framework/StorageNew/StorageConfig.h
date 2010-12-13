#ifndef STORAGECONFIG_H
#define STORAGECONFIG_H

#include "System/Common.h"

/*
===============================================================================================

 StorageJob

===============================================================================================
*/

#define STORAGE_DEFAULT_CHUNKSIZE           16*MiB
#define STORAGE_DEFAULT_LOGSEGMENTSIZE      16*MiB

class StorageConfig
{
public:
    StorageConfig();

    uint32_t        chunkSize;
    uint32_t        logSegmentSize;
};

#endif
