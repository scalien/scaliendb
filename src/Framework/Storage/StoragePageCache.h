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

    static void                 Clear();

    static uint64_t             GetSize();
    static unsigned             GetNumPages();
    
    static void                 AddMetaPage(StoragePage* page);
    static void                 AddDataPage(StoragePage* page, bool bulk = false);
    static void                 RemoveMetaPage(StoragePage* page);
    static void                 RemoveDataPage(StoragePage* page);
    static void                 RegisterMetaHit(StoragePage* page);
    static void                 RegisterDataHit(StoragePage* page);

private:
    static void                 RemoveOnePage();

    static uint64_t             size;
    static uint64_t             maxSize;
    static PageList             metaPages;
    static PageList             dataPages;
};

#endif
