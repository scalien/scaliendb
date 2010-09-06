#include "StorageDataCache.h"
#include "StorageFile.h"
#include "StorageDefaults.h"

#include <new>
#include <stdio.h>

static StorageDataCache* storageDataCache = NULL;

StorageDataCache::StorageDataCache()
{
	StorageDataPage* page;
	
	num = DEFAULT_CACHE_SIZE / DEFAULT_DATAPAGE_SIZE;

	pageArea = (StorageDataPage*) malloc(num * sizeof(StorageDataPage));
	for (unsigned i = 0; i < num; i++)
	{
		page = new ((void*) &pageArea[i]) StorageDataPage();
		freeList.Append(page);
	}
	
	bufferArea = (char*) malloc(num * DEFAULT_DATAPAGE_SIZE);
	for (unsigned i = 0; i < num; i++)
		pageArea[i].buffer.SetPreallocated(&bufferArea[i * DEFAULT_DATAPAGE_SIZE], DEFAULT_DATAPAGE_SIZE);
}

StorageDataCache* StorageDataCache::Get()
{
	if (storageDataCache == NULL)
		storageDataCache = new StorageDataCache;
	
	return storageDataCache;
}

StorageDataPage* StorageDataCache::GetPage()
{
	StorageDataPage*	page;
	StorageFile*		file;
	
//	return new StorageDataPage;
	
	if (freeList.GetLength() > 0)
	{
		page = freeList.First();
		freeList.Remove(page);
		return page;
	}
	
	// TODO: handle gracefully when ran out of memory
	assert(lruList.GetLength() > 0);
	
	page = lruList.Last();
	lruList.Remove(page);
	file = page->file;
	assert(file != NULL);
	
	file->UnloadDataPage(page);
	page->~StorageDataPage();
	page = new (page) StorageDataPage();
	return page;
}

void StorageDataCache::FreePage(StorageDataPage* page)
{
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
	static unsigned maxLength = 0;
	assert(page->next == page && page->prev == page);
	assert(page->dirty == false);
	lruList.Prepend(page);

//	if (lruList.GetLength() > maxLength)
//	{
//		maxLength = lruList.GetLength();
//		printf("lruList.length = %d\n", maxLength);
//	}
}

void StorageDataCache::Checkout(StorageDataPage* page)
{
	assert(page->next != page && page->prev != page);
	lruList.Remove(page);
}
