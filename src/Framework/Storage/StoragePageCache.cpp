#include "StoragePageCache.h"

StoragePageCache::PageList StoragePageCache::pages;
uint64_t StoragePageCache::size = 0;
uint64_t StoragePageCache::maxSize = 0;

void StoragePageCache::Init(StorageConfig& config)
{
    maxSize = config.fileChunkCacheSize;
}

void StoragePageCache::Shutdown()
{
    StoragePage*    it;
    
    for (it = pages.First(); it != NULL; /* advanced in body */)
    {
        pages.Remove(it);
        it->Unload();
        
        it = pages.First();
    }
}

uint64_t StoragePageCache::GetSize()
{
    return size;
}

unsigned StoragePageCache::GetNumPages()
{
    return pages.GetLength();
}

void StoragePageCache::AddPage(StoragePage* page, bool bulk)
{
    StoragePage*    it;

    while (size + page->GetSize() > maxSize)
    {
//        Log_Debug("removing page from cache");
        it = pages.First();
        size -= it->GetSize();
        pages.Remove(it);
        it->Unload();        
    }
    
    size += page->GetSize();

    if (bulk)
        pages.Prepend(page);
    else
        pages.Append(page);
}

void StoragePageCache::RemovePage(StoragePage* page)
{
    size -= page->GetSize();
    pages.Remove(page);
}

void StoragePageCache::RegisterHit(StoragePage* page)
{
    pages.Remove(page);
    pages.Append(page);
}
