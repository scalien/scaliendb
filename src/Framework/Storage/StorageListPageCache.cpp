#include "StorageListPageCache.h"
#include "StorageDataPage.h"
#include "System/Threading/Mutex.h"
#include "System/Containers/InList.h"

static Mutex dataPageCacheMutex;
InList<StorageDataPage> dataPageList;
int64_t cacheSize = 0;
int64_t maxCacheSize = 0;
int64_t maxUsedSize = 0;
int64_t maxCachedPageSize = 0;

void StorageListPageCache::SetMaxCacheSize(uint64_t maxCacheSize_)
{
    maxCacheSize = maxCacheSize_;
}

uint64_t StorageListPageCache::GetMaxCacheSize()
{
    return maxCacheSize;
}
uint64_t StorageListPageCache::GetCacheSize()
{
    return cacheSize;
}

uint64_t StorageListPageCache::GetMaxUsedSize()
{
    return maxUsedSize;
}

uint64_t StorageListPageCache::GetMaxCachedPageSize()
{
    return maxCachedPageSize;
}

unsigned StorageListPageCache::GetListLength()
{
    return dataPageList.GetLength();
}

void StorageListPageCache::Shutdown()
{
    MutexGuard mutexGuard(dataPageCacheMutex);

    dataPageList.DeleteList();
}

StorageDataPage* StorageListPageCache::Acquire()
{
    StorageDataPage* dataPage;

    ASSERT(maxCacheSize > 0);
    MutexGuard mutexGuard(dataPageCacheMutex);

    if (dataPageList.GetLength() > 0)
    {
        dataPage = dataPageList.Pop();
        cacheSize -= dataPage->GetMemorySize();
        ASSERT(cacheSize >= 0);
        return dataPage;
    }
    else
    {
        return new StorageDataPage();
    }
}

void StorageListPageCache::Release(StorageDataPage* dataPage)
{
    ASSERT(maxCacheSize > 0);
    MutexGuard mutexGuard(dataPageCacheMutex);

    if (cacheSize + dataPage->GetMemorySize() < maxCacheSize)
    {
        dataPageList.Append(dataPage);
        cacheSize += dataPage->GetMemorySize();
        if (cacheSize > maxUsedSize)
            maxUsedSize = cacheSize;
        if (dataPage->GetMemorySize() > maxCachedPageSize)
            maxCachedPageSize = dataPage->GetMemorySize();
    }
    else
    {
        delete dataPage;
    }
}
