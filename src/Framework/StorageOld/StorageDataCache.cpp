#include "StorageDataCache.h"
#include "StorageFile.h"
#include "StorageDefaults.h"

#include <new>
#include <stdio.h>

static StorageDataCache* storageDataCache = NULL;

StorageDataCache::StorageDataCache()
{
    num = 0;
}

void StorageDataCache::Init(uint64_t size)
{
    StorageDataPage*    page;
    size_t              bytes;
    
    ST_ASSERT(num == 0);

    num = size / DEFAULT_DATAPAGE_SIZE;

    bytes = num * sizeof(StorageDataPage);
    ST_ASSERT((uint64_t) bytes == num * (uint64_t) sizeof(StorageDataPage));
    pageArea = (StorageDataPage*) malloc(bytes);
    ST_ASSERT(pageArea != NULL);
    for (unsigned i = 0; i < num; i++)
    {
        page = new ((void*) &pageArea[i]) StorageDataPage();
        freeList.Append(page);
    }
    
    bytes = num * DEFAULT_DATAPAGE_SIZE;
    ST_ASSERT((uint64_t) bytes == num * (uint64_t) DEFAULT_DATAPAGE_SIZE);
    bufferArea = (char*) malloc(bytes);
    ST_ASSERT(bufferArea != NULL);
    for (unsigned i = 0; i < num; i++)
        pageArea[i].buffer.SetPreallocated(&bufferArea[i * DEFAULT_DATAPAGE_SIZE], DEFAULT_DATAPAGE_SIZE);
}

void StorageDataCache::Shutdown()
{
    ST_ASSERT(num != 0);

    lruList.Clear();
    freeList.Clear();
    
    free(pageArea);
    free(bufferArea);

    delete storageDataCache;
    storageDataCache = NULL;
}

uint64_t StorageDataCache::GetTotalSize()
{
    return num * (sizeof(StorageDataPage) + DEFAULT_DATAPAGE_SIZE);
}

uint64_t StorageDataCache::GetUsedSize()
{
    return (num - freeList.GetLength()) * (sizeof(StorageDataPage) + DEFAULT_DATAPAGE_SIZE);
}

StorageDataCache* StorageDataCache::Get()
{
    if (storageDataCache == NULL)
        storageDataCache = new StorageDataCache;
    
    return storageDataCache;
}

StorageDataPage* StorageDataCache::GetPage()
{
    StorageDataPage*    page;
    StorageFile*        file;
    
//  return new StorageDataPage;
    
    ST_ASSERT(lruList.GetLength() + freeList.GetLength() <= num);
    
    if (freeList.GetLength() > 0)
    {
        page = freeList.First();
        freeList.Remove(page);
        page = new (page) StorageDataPage();
        return page;
    }
    
    // TODO: handle gracefully when ran out of memory
    ST_ASSERT(lruList.GetLength() > 0);
    
    page = lruList.Last();
    lruList.Remove(page);
    file = page->file;
    ST_ASSERT(file != NULL);
    
    file->UnloadDataPage(page);
    page->~StorageDataPage();
    page = new (page) StorageDataPage();
    return page;
}

void StorageDataCache::FreePage(StorageDataPage* page)
{
    ST_ASSERT(lruList.GetLength() + freeList.GetLength() <= num);
    ST_ASSERT(page->next == page && page->prev == page);
    ST_ASSERT(page->detached == false);

    page->~StorageDataPage();
    freeList.Append(page);
}

void StorageDataCache::RegisterHit(StorageDataPage* page)
{
    Checkout(page);
    Checkin(page);
}

void StorageDataCache::Checkin(StorageDataPage* page)
{
    ST_ASSERT(lruList.GetLength() + freeList.GetLength() <= num);
    ST_ASSERT(page->next == page && page->prev == page);
    ST_ASSERT(page->dirty == false);
    ST_ASSERT(page->detached == false);

    lruList.Prepend(page);
}

void StorageDataCache::Checkout(StorageDataPage* page)
{
    ST_ASSERT(lruList.GetLength() + freeList.GetLength() <= num);
    ST_ASSERT(page->next != page && page->prev != page);

    lruList.Remove(page);
}
