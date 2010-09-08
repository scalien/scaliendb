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

void StorageDataCache::Init(unsigned size)
{
	StorageDataPage* page;
	
	assert(num == 0);
	num = size / DEFAULT_DATAPAGE_SIZE;

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

void StorageDataCache::Shutdown()
{
	assert(num != 0);
	
	lruList.Clear();
	freeList.Clear();
	
	free(pageArea);
	free(bufferArea);

	delete storageDataCache;
	storageDataCache = NULL;
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
	
	assert(lruList.GetLength() + freeList.GetLength() <= num);
	
	if (freeList.GetLength() > 0)
	{
		page = freeList.First();
		freeList.Remove(page);
		page = new (page) StorageDataPage();
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
	assert(lruList.GetLength() + freeList.GetLength() <= num);
	assert(page->next == page && page->prev == page);
	assert(page->detached == false);

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
	assert(lruList.GetLength() + freeList.GetLength() <= num);
	assert(page->next == page && page->prev == page);
	assert(page->dirty == false);
	assert(page->detached == false);

	lruList.Prepend(page);
}

void StorageDataCache::Checkout(StorageDataPage* page)
{
	assert(lruList.GetLength() + freeList.GetLength() <= num);
	assert(page->next != page && page->prev != page);

	lruList.Remove(page);
}
