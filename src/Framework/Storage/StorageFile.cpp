#include "StorageFile.h"
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdio.h> // REMOVE

#define DATAPAGE_OFFSET(idx) (INDEXPAGE_OFFSET+indexPageSize+idx*dataPageSize)

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
		delete dataPages[u];
	free(dataPages);
}

void StorageFile::Open(char* filepath_)
{
	struct stat st;

	filepath.Write(filepath_);
	filepath.NullTerminate();

	fd = open(filepath.GetBuffer(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);

	if (fd < 0)
		ASSERT_FAIL();

	fstat(fd, &st);
	if (st.st_size > 0)
		Read();
}

void StorageFile::Close()
{
	close(fd);
	
	fd = -1;
}

void StorageFile::SetFileIndex(uint32_t fileIndex_)
{
	StoragePage*	it;
	
	fileIndex = fileIndex_;
	
	indexPage.SetFileIndex(fileIndex);
	for (it = dirtyPages.First(); it != NULL; it = dirtyPages.Next(it))
		it->SetFileIndex(fileIndex);
}

//bool StorageFile::IsNew()
//{
//	return newFile;
//}

bool StorageFile::Get(ReadBuffer& key, ReadBuffer& value)
{
	int32_t index;
	
	index = Locate(key);
	
	if (index < 0)
		return false;
	
	return dataPages[index]->Get(key, value);
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
		index = 0;
		assert(numDataPages == 0);
		assert(dataPages[index] == NULL);
		dataPages[index] = new StorageDataPage;
		dataPages[index]->SetFileIndex(fileIndex);
		dataPages[index]->SetOffset(DATAPAGE_OFFSET(index));
		dataPages[index]->SetPageSize(dataPageSize);
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
		dataPages[index]->Detach();
		assert(dataPages[index]->HasCursors() == false);
	}
	
	dataPages[index]->Set(key, value, copy);
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

void StorageFile::Read()
{
	char*			p;
	unsigned		i;
	int				length;
	Buffer			buffer;
	ReadBuffer		readBuffer;
	
	newFile = false;

	buffer.Allocate(12);
	if (pread(fd, (void*) buffer.GetBuffer(), 12, 0) < 0)
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
	
	buffer.Allocate(indexPageSize);
	length = pread(fd, (void*) buffer.GetBuffer(), indexPageSize, INDEXPAGE_OFFSET);
	if (length < 0)
		ASSERT_FAIL();
	buffer.SetLength(length);
	readBuffer.Wrap(buffer);
	indexPage.Read(readBuffer);
	if (dataPages != NULL)
		free(dataPages);
	dataPages = (StorageDataPage**) malloc(sizeof(StorageDataPage*) * numDataPageSlots);
	for (i = 0; i < numDataPageSlots; i++)
		dataPages[i] = NULL;
	numDataPages = indexPage.NumEntries();	
}

void StorageFile::ReadRest()
{
	KeyIndex*		it;

	// TODO make sure this IO occurs in order!
	for (it = indexPage.keys.First(); it != NULL; it = indexPage.keys.Next(it))
		if (dataPages[it->index] == NULL)
			LoadDataPage(it->index);
}

void StorageFile::WriteRecovery(int recoveryFD)
{
	StoragePage*	it;

	for (it = dirtyPages.First(); it != NULL; it = dirtyPages.Next(it))
	{
		if (it->IsNew())
			continue;
		assert(it->buffer.GetLength() <= it->GetPageSize());
		// it->buffer contains the old page
		if (write(recoveryFD, it->buffer.GetBuffer(), it->GetPageSize()) < 0)
		{
			Log_Errno();
			ASSERT_FAIL();
		}
	}
}

void StorageFile::WriteData()
{
	Buffer			buffer;
	StoragePage*	it;
	char*			p;

	if (newFile)
	{
		buffer.Allocate(12);
		p = buffer.GetBuffer();
		*((uint32_t*) p) = ToLittle32(indexPageSize);
		p += 4;
		*((uint32_t*) p) = ToLittle32(dataPageSize);
		p += 4;
		*((uint32_t*) p) = ToLittle32(numDataPageSlots);
		p += 4;
		
		if (pwrite(fd, (const void *) buffer.GetBuffer(), 12, 0) < 0)
			ASSERT_FAIL();
		
		newFile = false;
	}
	
	for (it = dirtyPages.First(); it != NULL; it = dirtyPages.Remove(it))
	{
		buffer.Allocate(it->GetPageSize());
 		buffer.Zero();
//		printf("writing file %s at offset %u\n", filepath.GetBuffer(), it->GetOffset());
		it->Write(buffer);
		if (pwrite(fd, buffer.GetBuffer(), it->GetPageSize(), it->GetOffset()) < 0)
			ASSERT_FAIL();
		it->SetDirty(false);
		it->SetNew(false);
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
	
	dataPages[index] = new StorageDataPage;
	dataPages[index]->SetOffset(DATAPAGE_OFFSET(index));
	dataPages[index]->SetPageSize(dataPageSize);
	dataPages[index]->SetNew(false);
	
//	printf("loading data page from %s at index %u\n", filepath.GetBuffer(), index);

	buffer.Allocate(dataPageSize);
//	printf("reading page %u from %u\n", index, DATAPAGE_OFFSET(index));
	length = pread(fd, buffer.GetBuffer(), dataPageSize, DATAPAGE_OFFSET(index));
	if (length < 0)
		ASSERT_FAIL();
	buffer.SetLength(length);
	readBuffer.Wrap(buffer);
	dataPages[index]->Read(readBuffer);
}

void StorageFile::MarkPageDirty(StoragePage* page)
{
	if (!page->IsDirty())
	{
		page->SetDirty(true);
		dirtyPages.Insert(page);
	}
}

void StorageFile::SplitDataPage(uint32_t index)
{
	uint32_t			newIndex;
	StorageDataPage*	newPage;

	if (numDataPages < numDataPageSlots)
	{
		// make a copy of data
		newPage = dataPages[index]->SplitDataPage();
		newPage->SetFileIndex(fileIndex);
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
//			if (newPage->MustSplit())
//			{
//				TODO
//			}
	}
	else
		isOverflowing = true;
}

void StorageFile::ReorderPages()
{
	uint32_t			newIndex, oldIndex, i;
	KeyIndex*			it;
	StorageDataPage**	newDataPages;
	
	newDataPages = (StorageDataPage**) calloc(numDataPageSlots, sizeof(StorageDataPage*));
	
	for (it = indexPage.keys.First(), newIndex = 0; it != NULL; it = indexPage.keys.Next(it), newIndex++)
	{
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
		dataPages[index]->SetDirty(false);
		MarkPageDirty(dataPages[index]);
	}
}
