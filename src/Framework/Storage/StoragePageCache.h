#ifndef STORAGEPAGECACHE_H
#define STORAGEPAGECACHE_H

#include "System/Containers/InList.h"
#include "StoragePage.h"
#include "StorageConfig.h"

/*
===============================================================================================

 StoragePageCache

===============================================================================================
*/

class StoragePageCache
{
    typedef InList<StoragePage> PageList;

public:
    static void                 Init(StorageConfig& config);
    static void                 Shutdown();

    static uint64_t             GetSize();
    static void                 AddPage(StoragePage* page, bool bulk = false);
    static void                 RemovePage(StoragePage* page);
    static void                 RegisterHit(StoragePage* page);

private:
    static uint64_t             size;
    static uint64_t             maxSize;
    static PageList             pages;
};

#endif
