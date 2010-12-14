#include "StoragePageCache.h"

StoragePageCache::PageList StoragePageCache::pages;
uint64_t StoragePageCache::size = 0;

uint64_t StoragePageCache::GetSize()
{
    return size;
}

void StoragePageCache::AddPage(StoragePage* page)
{
    size += page->GetSize();
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

void StoragePageCache::TryUnloadPages(StorageConfig& config)
{
    StoragePage*    it;
    uint64_t        numUnloaded;
    
    numUnloaded = 0;
    for (it = pages.First(); it != NULL; /* advanced in body */)
    {
        if (size < config.fileChunkCacheSize)
            break;
        
        size -= it->GetSize();
        pages.Remove(it);
        it->Unload();
        
        numUnloaded++;
        it = pages.First();
    }
    
    if (numUnloaded > 0)
    {
        Log_Message("Unloaded %U pages from cache...", numUnloaded);
    }
}
