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
    Clear();
}

void StoragePageCache::Clear()
{
    StoragePage*    it;
    
    FOREACH_FIRST (it, pages)
    {
        pages.Remove(it);
        it->Unload();
        
        it = pages.First();
    }

    size = 0;
    
    Log_Debug("Cache cleared");
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

    while (size + page->GetMemorySize() > maxSize)
    {
        it = pages.First();
        size -= it->GetMemorySize();
        pages.Remove(it);
        it->Unload();        
    }
    
    size += page->GetMemorySize();

    if (bulk)
        pages.Prepend(page);
    else
        pages.Append(page);
}

void StoragePageCache::RemovePage(StoragePage* page)
{
    size -= page->GetMemorySize();
    pages.Remove(page);
}

void StoragePageCache::RegisterHit(StoragePage* page)
{
    pages.Remove(page);
    pages.Append(page);
}
