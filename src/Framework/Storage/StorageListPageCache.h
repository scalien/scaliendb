#ifndef STORAGELISTPAGECACHE_H
#define STORAGELISTPAGECACHE_H

#include "System/Platform.h"

class StorageDataPage;

/*
===============================================================================================

 StorageListPageCache

===============================================================================================
*/

class StorageListPageCache
{
public:
    static void                 SetMaxCacheSize(uint64_t maxCacheSize);
    static uint64_t             GetMaxCacheSize();
    static uint64_t             GetCacheSize();
    static uint64_t             GetMaxUsedSize();
    static uint64_t             GetMaxCachedPageSize();
    static unsigned             GetListLength();
    static void                 Shutdown();
    static StorageDataPage*     Acquire();
    static void                 Release(StorageDataPage* dataPage);
};

#endif
