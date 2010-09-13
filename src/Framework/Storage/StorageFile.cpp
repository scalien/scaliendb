#include "StorageFile.h"
#include "StorageFileHeader.h"
#include "StorageDataCache.h"
#include "System/FileSystem.h"
#include "stdio.h"

#define INDEXPAGE_OFFSET	(STORAGEFILE_HEADER_LENGTH+INDEXPAGE_HEADER_SIZE)

#define DATAPAGE_OFFSET(idx) (INDEXPAGE_OFFSET+indexPageSize+(idx)*dataPageSize)
#define DATAPAGE_INDEX(offs) (((offs)-INDEXPAGE_OFFSET-indexPageSize)/dataPageSize)
#define FILE_TYPE			"ScalienDB data file"
#define FILE_VERSION_MAJOR	0
#define FILE_VERSION_MINOR	1

static int KeyCmp(const uint32_t a, const uint32_t b)
{
	if (a < b)
		return -1;
	else if (a > b)
		return 1;
	else
		return 0;		
}

static uint32_t Key(StoragePage* page)
{
	return page->GetOffset();
}

StorageFile::StorageFile()
{
	uint32_t i;
		
	indexPageSize = DEFAULT_INDEXPAGE_SIZE;
	dataPageSize = DEFAULT_DATAPAGE_SIZE;
	numDataPageSlots = DEFAULT_NUM_DATAPAGES;

	isOverflowing = false;
	newFile = true;
	
	indexPage.SetOffset(INDEXPAGE_OFFSET);
	indexPage.SetPageSize(indexPageSize);
	indexPage.SetNumDataPageSlots(numDataPageSlots);

	dataPages = (StorageDataPage**) malloc(sizeof(StorageDataPage*) * numDataPageSlots);
	for (i = 0; i < numDataPageSlots; i++)
		dataPages[i] = NULL;
	numDataPages = 0;
	
	fd = -1;
}

StorageFile::~StorageFile()
{
	// dirtyPages' destructor automatically calls this
	// but it is only running after all pages are free'd.
	dirtyPages.Clear();
	
	for (uint32_t u = 0; u < numDataPages; u++)
	{
		if (dataPages[u] != NULL)
		{
			if (!dataPages[u]->IsDirty())
				DCACHE->Checkout(dataPages[u]);

			DCACHE->FreePage(dataPages[u]);
		}
	}
	free(dataPages);
}

void StorageFile::Open(const char* filepath_)
{
	filepath.Write(filepath_);
	filepath.NullTerminate();

	fd = FS_Open(filepath.GetBuffer(), FS_READWRITE | FS_CREATE);

	if (fd == INVALID_FD)
		ASSERT_FAIL();

	if (FS_FileSize(fd) > 0)
		Read();
}

void StorageFile::Close()
{
	FS_FileClose(fd);
	
	fd = INVALID_FD;
}

void StorageFile::SetStorageFileIndex(uint32_t fileIndex_)
{
	StoragePage*	it;
	
	fileIndex = fileIndex_;
	
	indexPage.SetStorageFileIndex(fileIndex);
	for (it = dirtyPages.First(); it != NULL; it = dirtyPages.Next(it))
		it->SetStorageFileIndex(fileIndex);
}

bool StorageFile::Get(ReadBuffer& key, ReadBuffer& value)
{
	int32_t index;
	bool	ret;
	
	index = Locate(key);
	
	if (index < 0)
		return false;
	
	ret = dataPages[index]->Get(key, value);
	if (ret)
		DCACHE->RegisterHit(dataPages[index]);
	return ret;
}

bool StorageFile::Set(ReadBuffer& key, ReadBuffer& value, bool copy)
{
	int32_t		index;
	ReadBuffer	rb;
	
	if (key.GetLength() + value.GetLength() > DATAPAGE_MAX_KV_SIZE(dataPageSize))
		return false;
	
	index = Locate(key);
	
	if (index < 0)
	{
		// file is empty, allocate first data page
		index = 0;
		assert(numDataPages == 0);
		assert(dataPages[index] == NULL);
		dataPages[index] = DCACHE->GetPage();
		DCACHE->Checkin(dataPages[index]);
		dataPages[index]->SetStorageFileIndex(fileIndex);
		dataPages[index]->SetOffset(DATAPAGE_OFFSET(index));
		dataPages[index]->SetPageSize(dataPageSize);
		dataPages[index]->SetFile(this);
		numDataPages++;
		indexPage.Add(key, index, true); // TODO
		MarkPageDirty(&indexPage);
	}
	else
	{
		rb = dataPages[index]->FirstKey();
		if (ReadBuffer::LessThan(key, rb))
		{
//			printf("Changing index entry for %u to %.*s", index, P(&key));
			indexPage.Update(key, index, true);
			MarkPageDirty(&indexPage);
		}
	}
	
	if (dataPages[index]->HasCursors())
	{
		// no cache because cursor has the detached copy
		dataPages[index]->Detach();
		assert(dataPages[index]->HasCursors() == false);
	}
	
	if (!dataPages[index]->Set(key, value, copy))
	{
		if (!dataPages[index]->IsDirty())
			DCACHE->RegisterHit(dataPages[index]);
		return true; // nothing changed
	}
	
	MarkPageDirty(dataPages[index]);
	
	if (dataPages[index]->IsOverflowing())
		SplitDataPage(index);
	
	return true;
}

void StorageFile::Delete(ReadBuffer& key)
{
	bool		updateIndex;
	int32_t		index;
	ReadBuffer	firstKey;
	
	index = Locate(key);
	
	if (index < 0)
		return;

	updateIndex = false;
	firstKey = dataPages[index]->FirstKey();
	if (BUFCMP(&key, &firstKey))
		updateIndex = true;

	if (dataPages[index]->HasCursors())
	{
		dataPages[index]->Detach();
		assert(dataPages[index]->HasCursors() == false);
	}

	dataPages[index]->Delete(key);
	MarkPageDirty(dataPages[index]);

	if (dataPages[index]->IsEmpty())
	{
		indexPage.Remove(key);
		MarkPageDirty(&indexPage);
	}
	else if (updateIndex)
	{
		firstKey = dataPages[index]->FirstKey();
		indexPage.Update(firstKey, index, true);
		MarkPageDirty(&indexPage);		
	}
}

ReadBuffer StorageFile::FirstKey()
{
	return indexPage.FirstKey();
}

bool StorageFile::IsEmpty()
{
	return indexPage.IsEmpty();
}

bool StorageFile::IsNew()
{
	return newFile;
}

bool StorageFile::IsOverflowing()
{
	return isOverflowing || indexPage.IsOverflowing();
}

StorageFile* StorageFile::SplitFile()
{
	StorageFile*	newFile;
	uint32_t		index, newIndex, num;
	
	assert(numDataPageSlots == numDataPages);
	
	// TODO: do thing for cursors
	
	newFile = new StorageFile;
	newFile->indexPageSize = indexPageSize;
	newFile->dataPageSize = dataPageSize;
	newFile->numDataPageSlots = numDataPageSlots;
	newFile->indexPage.SetPageSize(indexPageSize);
	newFile->indexPage.SetNumDataPageSlots(numDataPageSlots);
	
	ReorderPages();
	
	num = numDataPages;
	for (index = numDataPageSlots / 2, newIndex = 0; index < num; index++, newIndex++)
	{
		assert(dataPages[index]->IsEmpty() != true);
		newFile->dataPages[newIndex] = dataPages[index];
		dataPages[index] = NULL;
		newFile->dataPages[newIndex]->SetOffset(DATAPAGE_OFFSET(newIndex));
		newFile->dataPages[newIndex]->SetFile(newFile);

		numDataPages--;
		newFile->numDataPages++;
		assert(newFile->numDataPages < newFile->numDataPageSlots);

		indexPage.Remove(newFile->dataPages[newIndex]->FirstKey());
		newFile->indexPage.Add(newFile->dataPages[newIndex]->FirstKey(), newIndex, true);
	}
	
	isOverflowing = false;
	for (index = 0; index < numDataPages; index++)
	{
		if (dataPages[index]->IsOverflowing())
			SplitDataPage(index);
	}
	assert(isOverflowing == false);
	
	newFile->isOverflowing = false;
	for (index = 0; index < newFile->numDataPages; index++)
	{
		if (newFile->dataPages[index]->IsOverflowing())
			newFile->SplitDataPage(index);
	}
	assert(newFile->isOverflowing == false);
	
	ReorderFile();
	newFile->ReorderFile();
		
	return newFile;
}

StorageFile* StorageFile::SplitFileByKey(ReadBuffer& startKey)
{
	StorageDataPage*	page;
	StorageDataPage*	newPage;
	StorageFile*		newFile;
	uint32_t			index;
	uint32_t			splitIndex;
	int32_t				ret;
	int					cmp;
	
	newFile = new StorageFile;
	newFile->indexPageSize = indexPageSize;
	newFile->dataPageSize = dataPageSize;
	newFile->numDataPageSlots = numDataPageSlots;
	newFile->indexPage.SetPageSize(indexPageSize);
	newFile->indexPage.SetNumDataPageSlots(numDataPageSlots);

	ReadRest();
	ReorderPages();
	
	ret = Locate(startKey);
	assert(ret >= 0);
	splitIndex = (uint32_t) ret;
	
	for (index = 0; index < numDataPageSlots; index++)
	{
		page = dataPages[index];
		if (page == NULL)
			break;			// the pages are already ordered
	
		cmp = ReadBuffer::Cmp(startKey, page->FirstKey());
		if (cmp <= 0)
		{
			if (cmp < 0 && index == splitIndex)
			{
				// startKey is not the first key of the page so we have to split it
				newPage = page->SplitDataPageByKey(startKey);
				newFile->dataPages[index] = newPage;
			}
			else
			{
				newFile->dataPages[index] = dataPages[index];
				indexPage.Remove(dataPages[index]->FirstKey()); 
				dataPages[index] = NULL;
				numDataPages--;
			}

			newFile->dataPages[index]->SetOffset(DATAPAGE_OFFSET(index));
			newFile->dataPages[index]->SetFile(newFile);
			newFile->dataPages[index]->Invalidate();
			MarkPageDirty(newFile->dataPages[index]);
			newFile->indexPage.Add(newFile->dataPages[index]->FirstKey(), index, true);		
			newFile->numDataPages++;
		}
	}
	
	ReorderFile();
	newFile->ReorderFile();
	
	return newFile;
}

void StorageFile::Read()
{
	char*				p;
	unsigned			i;
	int					length;
	Buffer				buffer;
	ReadBuffer			readBuffer;
	StorageFileHeader	header;
	
	newFile = false;

	// read file header
	buffer.Allocate(STORAGEFILE_HEADER_LENGTH);
	if (FS_FileRead(fd, (void*) buffer.GetBuffer(), STORAGEFILE_HEADER_LENGTH) < 0)
		ASSERT_FAIL();
	buffer.SetLength(STORAGEFILE_HEADER_LENGTH);
	if (!header.Read(buffer))
		ASSERT_FAIL();
	
	// read index header
	buffer.Allocate(INDEXPAGE_HEADER_SIZE);
	if (FS_FileRead(fd, (void*) buffer.GetBuffer(), INDEXPAGE_HEADER_SIZE) < 0)
		ASSERT_FAIL();
	p = buffer.GetBuffer();
	indexPageSize = FromLittle32(*((uint32_t*) p));
	p += 4;
	dataPageSize = FromLittle32(*((uint32_t*) p));
	p += 4;
	numDataPageSlots = FromLittle32(*((uint32_t*) p));
	p += 4;

	indexPage.SetOffset(INDEXPAGE_OFFSET);
	indexPage.SetPageSize(indexPageSize);
	indexPage.SetNew(false);
	indexPage.SetNumDataPageSlots(numDataPageSlots);
	
	// read index page
	buffer.Allocate(indexPageSize);
	length = FS_FileRead(fd, (void*) buffer.GetBuffer(), indexPageSize);
	if (length < 0)
		ASSERT_FAIL();
	buffer.SetLength(length);
	readBuffer.Wrap(buffer);
	indexPage.Read(readBuffer);
	
	// allocate memory for data page slots
	if (dataPages != NULL)
		free(dataPages);
	dataPages = (StorageDataPage**) malloc(sizeof(StorageDataPage*) * numDataPageSlots);
	for (i = 0; i < numDataPageSlots; i++)
		dataPages[i] = NULL;
	numDataPages = indexPage.NumEntries();	
}

void StorageFile::ReadRest()
{
	StorageKeyIndex*		it;
	uint32_t*				uit;
	SortedList<uint32_t>	indexes;

	// IO occurs in order

	for (it = indexPage.keys.First(); it != NULL; it = indexPage.keys.Next(it))
		if (dataPages[it->index] == NULL)
			indexes.Add(it->index);

	for (uit = indexes.First(); uit != NULL; uit = indexes.Next(uit))
		LoadDataPage(*uit);
}

void StorageFile::WriteRecovery(FD recoveryFD)
{
	Buffer			buffer;
	StoragePage*	it;

	for (it = dirtyPages.First(); it != NULL; it = dirtyPages.Next(it))
	{
		if (it->IsNew())
			continue;
		assert(it->buffer.GetLength() <= it->GetPageSize());
		// it->buffer contains the old page
		buffer.Allocate(it->GetPageSize());
		if (!it->CheckWrite(buffer))
			continue;
		if (FS_FileWrite(recoveryFD, it->buffer.GetBuffer(), it->buffer.GetLength()) < 0)
		{
			Log_Errno();
			ASSERT_FAIL();
		}
		FS_FileSeek(recoveryFD, it->GetPageSize() - it->buffer.GetLength(), FS_SEEK_CUR);
	}
}

void StorageFile::WriteData()
{
	Buffer					buffer;
	StoragePage*			it;
	char*					p;
	InList<StoragePage>		dirties;
	StoragePage*			next;
	StorageFileHeader		header;
	
	if (newFile)
	{
		buffer.Allocate(STORAGEFILE_HEADER_LENGTH);
		header.Init(FILE_TYPE, FILE_VERSION_MAJOR, FILE_VERSION_MINOR, 0);
		header.Write(buffer);
		if (FS_FileWrite(fd, (const void *) buffer.GetBuffer(), STORAGEFILE_HEADER_LENGTH) < 0)
			ASSERT_FAIL();
		
		buffer.Allocate(INDEXPAGE_HEADER_SIZE);
		p = buffer.GetBuffer();
		*((uint32_t*) p) = ToLittle32(indexPageSize);
		p += 4;
		*((uint32_t*) p) = ToLittle32(dataPageSize);
		p += 4;
		*((uint32_t*) p) = ToLittle32(numDataPageSlots);
		p += 4;
		
		if (FS_FileWrite(fd, (const void *) buffer.GetBuffer(), INDEXPAGE_HEADER_SIZE) < 0)
			ASSERT_FAIL();
		
		newFile = false;
	}
	
	for (it = dirtyPages.First(); it != NULL; it = dirtyPages.Remove(it))
	{
		buffer.Allocate(it->GetPageSize());
 		buffer.Zero();
//		printf("writing file %s at offset %u\n", filepath.GetBuffer(), it->GetOffset());
		if (it->Write(buffer))
		{
			if (FS_FileWriteOffs(fd, buffer.GetBuffer(), it->GetPageSize(), it->GetOffset()) < 0)
				ASSERT_FAIL();
		}
		it->SetDirty(false);
		it->SetNew(false);

		dirties.Append(it);
	}
	
	for (it = dirties.First(); it != NULL; it = next)
	{
		next = dirties.Remove(it);
		if (it->GetType() == STORAGE_DATA_PAGE)
			DCACHE->Checkin((StorageDataPage*) it);
	}
}

StorageDataPage* StorageFile::CursorBegin(ReadBuffer& key, Buffer& nextKey)
{
	int32_t	index;
	
	index = indexPage.Locate(key, &nextKey);
	
	if (index < 0)	// file is empty
		return NULL;
	
	if (dataPages[index] == NULL)
		LoadDataPage(index);
	
	return dataPages[index];
}

uint64_t StorageFile::GetSize()
{
	uint64_t	size;
	
	size = STORAGEFILE_HEADER_LENGTH + INDEXPAGE_HEADER_SIZE + indexPageSize +
			(indexPage.GetMaxDataPageIndex() + 1) * dataPageSize;

	return size;
}

int32_t StorageFile::Locate(ReadBuffer& key)
{
	int32_t	index;
	Buffer	nextKey;
	
	index = indexPage.Locate(key, &nextKey);
	
	if (index < 0)	// file is empty
		return index;
	
	if (dataPages[index] == NULL)
		LoadDataPage(index);
	
	return index;
}

void StorageFile::LoadDataPage(uint32_t index)
{
	Buffer		buffer;
	ReadBuffer	readBuffer;
	int			length;
	
	// load existing data page from disk
	dataPages[index] = DCACHE->GetPage();
	DCACHE->Checkin(dataPages[index]);

	dataPages[index]->SetOffset(DATAPAGE_OFFSET(index));
	dataPages[index]->SetPageSize(dataPageSize);
	dataPages[index]->SetNew(false);
	dataPages[index]->SetFile(this);
		
//	printf("loading data page from %s at index %u\n", filepath.GetBuffer(), index);

	buffer.Allocate(dataPageSize);
//	printf("reading page %u from %u\n", index, DATAPAGE_OFFSET(index));
	length = FS_FileReadOffs(fd, buffer.GetBuffer(), dataPageSize, DATAPAGE_OFFSET(index));
	if (length < 0)
		ASSERT_FAIL();
	buffer.SetLength(length);
	readBuffer.Wrap(buffer);
	dataPages[index]->Read(readBuffer);
}

void StorageFile::UnloadDataPage(StorageDataPage* page)
{
	int32_t	index;
	
	index = DATAPAGE_INDEX(page->GetOffset());
	assert(dataPages[index] == page);
	dataPages[index] = NULL;
}

void StorageFile::MarkPageDirty(StoragePage* page)
{
	if (!page->IsDirty())
	{
		// TODO: HACK
		if (page->GetType() == STORAGE_DATA_PAGE)
			DCACHE->Checkout((StorageDataPage*) page);
		
		page->SetDirty(true);
		dirtyPages.Insert(page);
	}
}

void StorageFile::SplitDataPage(uint32_t index)
{
	uint32_t			newIndex;
	StorageDataPage*	newPage;

	// TODO: split into three when one key-value has pagesize size
	if (numDataPages < numDataPageSlots)
	{
		// make a copy of data
		newPage = dataPages[index]->SplitDataPage();
		newPage->SetStorageFileIndex(fileIndex);
		newPage->SetFile(this);
		numDataPages++;
		assert(numDataPages <= numDataPageSlots);
		newIndex = indexPage.NextFreeDataPage();
		newPage->SetOffset(DATAPAGE_OFFSET(newIndex));
		assert(dataPages[newIndex] == NULL);
		dataPages[newIndex] = newPage;
		indexPage.Add(newPage->FirstKey(), newIndex, true); // TODO
		assert(dataPages[index]->IsDirty() == true);
		assert(newPage->IsDirty() == false);
		MarkPageDirty(newPage);
		MarkPageDirty(&indexPage);
	}
	else
		isOverflowing = true;
}

void StorageFile::ReorderPages()
{
	uint32_t			newIndex, oldIndex, i;
	StorageKeyIndex*	it;
	StorageDataPage**	newDataPages;
	
	newDataPages = (StorageDataPage**) calloc(numDataPageSlots, sizeof(StorageDataPage*));
	
	for (it = indexPage.keys.First(), newIndex = 0; it != NULL; it = indexPage.keys.Next(it), newIndex++)
	{
		assert(newIndex < numDataPageSlots);
		oldIndex = it->index;
		assert(dataPages[oldIndex] != NULL);
		it->index = newIndex;
		newDataPages[newIndex] = dataPages[oldIndex];
		newDataPages[newIndex]->SetOffset(DATAPAGE_OFFSET(newIndex));
	}
	
	free(dataPages);
	dataPages = newDataPages;
	
	indexPage.freeDataPages.Clear();
	for (i = numDataPages; i < numDataPageSlots; i++)
		indexPage.freeDataPages.Add(i);
}

void StorageFile::ReorderFile()
{
	uint32_t index;
	
	dirtyPages.Clear();
	ReorderPages();
	indexPage.SetDirty(false);
	MarkPageDirty(&indexPage);
	for (index = 0; index < numDataPages; index++)
	{
		// HACK: if it was dirty, it must be checked in before marked again as dirty
		if (dataPages[index]->IsDirty())
		{
			dataPages[index]->SetDirty(false);
			DCACHE->Checkin(dataPages[index]);
		}
		else
			dataPages[index]->SetDirty(false);

		MarkPageDirty(dataPages[index]);
	}
	
	// truncate back
	if (numDataPages != numDataPageSlots)
	{
		if (fd != INVALID_FD)
			FS_FileTruncate(fd, DATAPAGE_OFFSET(numDataPages));
	}
}

