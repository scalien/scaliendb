#include "StoragePageCache.h"
#include "System/Registry.h"

StoragePageCache::PageList StoragePageCache::metaPages;
StoragePageCache::PageList StoragePageCache::dataPages;
uint64_t StoragePageCache::size = 0;
uint64_t StoragePageCache::maxSize = 0;
static uint64_t*    numMetaPageHits;
static uint64_t*    numMetaPageMisses;
static uint64_t*    numDataPageHits;
static uint64_t*    numDataPageMisses;

void StoragePageCache::Init(StorageConfig& config)
{
    maxSize = config.GetFileChunkCacheSize();

    numMetaPageHits = Registry::GetUintPtr("storage.pageCache.numMetaPageHits");
    numMetaPageMisses = Registry::GetUintPtr("storage.pageCache.numMetaPageMisses");
    numDataPageHits = Registry::GetUintPtr("storage.pageCache.numDataPageHits");
    numDataPageMisses = Registry::GetUintPtr("storage.pageCache.numDataPageMisses");
}

void StoragePageCache::Shutdown()
{
    Clear();
}

void StoragePageCache::Clear()
{
    StoragePage*    it;
    
    FOREACH_FIRST (it, metaPages)
    {
        metaPages.Remove(it);
        it->Unload();
        
        it = metaPages.First();
    }

    FOREACH_FIRST (it, dataPages)
    {
        dataPages.Remove(it);
        it->Unload();
        
        it = dataPages.First();
    }

    size = 0;
    
    Log_Message("Page cache cleared");
}

uint64_t StoragePageCache::GetSize()
{
    return size;
}

unsigned StoragePageCache::GetNumPages()
{
    return metaPages.GetLength() + dataPages.GetLength();
}

void StoragePageCache::AddMetaPage(StoragePage* page)
{
    ASSERT(page->GetMemorySize() < maxSize);

    while (size + page->GetMemorySize() > maxSize)
        RemoveOnePage();

    size += page->GetMemorySize();
    metaPages.Append(page);

    *numMetaPageMisses += 1;
}

void StoragePageCache::AddDataPage(StoragePage* page, bool bulk)
{
    ASSERT(page->GetMemorySize() < maxSize);

    while (size + page->GetMemorySize() > maxSize)
        RemoveOnePage();
    
    size += page->GetMemorySize();

    if (bulk)
        dataPages.Prepend(page);
    else
        dataPages.Append(page);

    *numDataPageMisses += 1;
}

void StoragePageCache::RemoveMetaPage(StoragePage* page)
{
    size -= page->GetMemorySize();
    metaPages.Remove(page);
}

void StoragePageCache::RemoveDataPage(StoragePage* page)
{
    size -= page->GetMemorySize();
    dataPages.Remove(page);
}

void StoragePageCache::RegisterMetaHit(StoragePage* page)
{
    metaPages.Remove(page);
    metaPages.Append(page);

    *numMetaPageHits += 1;
}

void StoragePageCache::RegisterDataHit(StoragePage* page)
{
    dataPages.Remove(page);
    dataPages.Append(page);

    *numDataPageHits += 1;
}

void StoragePageCache::RemoveOnePage()
{
    StoragePage* page;

    if (dataPages.GetLength() > 0)
    {
        page = dataPages.First();
        ASSERT(page);
        size -= page->GetMemorySize();
        dataPages.Remove(page);
        page->Unload();
    }
    else if (metaPages.GetLength() > 0)
    {
        page = metaPages.First();
        ASSERT(page);
        size -= page->GetMemorySize();
        metaPages.Remove(page);
        page->Unload();
    }
}