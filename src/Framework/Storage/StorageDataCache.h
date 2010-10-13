#ifndef STORAGEDATACACHE_H
#define STORAGEDATACACHE_H

#include "System/Containers/InList.h"
#include "System/Buffers/Buffer.h"
#include "StorageDataPage.h"

#define DCACHE (StorageDataCache::Get())

/*
===============================================================================

 StorageDataCache

===============================================================================
*/

class StorageDataCache
{
public:
    static StorageDataCache*    Get();
    
    void                        Init(uint64_t size);
    void                        Shutdown();

    uint64_t                    GetTotalSize();
    uint64_t                    GetUsedSize();

    StorageDataPage*            GetPage();
    void                        FreePage(StorageDataPage* page);

    void                        RegisterHit(StorageDataPage* page);
    void                        Checkin(StorageDataPage* page);
    void                        Checkout(StorageDataPage* page);

private:
    StorageDataCache();
    
    InList<StorageDataPage>     freeList;
    InList<StorageDataPage>     lruList;
    StorageDataPage*            pageArea;
    char*                       bufferArea;
    uint64_t                    num;
};

#endif
