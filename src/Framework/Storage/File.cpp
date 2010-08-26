#include "File.h"
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdio.h> // REMOVE

#define DATAPAGE_OFFSET(idx) (INDEXPAGE_OFFSET+indexPageSize+idx*dataPageSize)

File::File()
{
	uint32_t i;
	
	prev = this;
	next = this;
	
	indexPageSize = DEFAULT_INDEXPAGE_SIZE;
	dataPageSize = DEFAULT_DATAPAGE_SIZE;
	numDataPageSlots = DEFAULT_NUM_DATAPAGES;

	isOverflowing = false;
	newFile = true;
	
	indexPage.SetOffset(INDEXPAGE_OFFSET);
	indexPage.SetPageSize(indexPageSize);
	indexPage.SetNumDataPageSlots(numDataPageSlots);

	dataPages = (DataPage**) malloc(sizeof(DataPage*) * numDataPageSlots);
	for (i = 0; i < numDataPageSlots; i++)
		dataPages[i] = NULL;
	numDataPages = 0;
	
	fd = -1;
}

File::~File()
{
	for (uint32_t u = 0; u < numDataPages; u++)
		delete dataPages[u];
	free(dataPages);
}

void File::Open(char* filepath_)
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

void File::Flush()
{
}

void File::Close()
{
	close(fd);
	
	fd = -1;
}

bool File::Get(ReadBuffer& key, ReadBuffer& value)
{
	int32_t index;
	
	index = Locate(key);
	
	if (index < 0)
		return false;
	
	return dataPages[index]->Get(key, value);
}

bool File::Set(ReadBuffer& key, ReadBuffer& value, bool copy)
{
	int32_t		index;
	ReadBuffer	rb;
	
	if (key.GetLength() + value.GetLength() > DATAPAGE_MAX_KV_SIZE(dataPageSize))
		return false;
	
	index = Locate(key);
	
	if (index < 0)
	{
		index = 0;
		assert(dataPages[index] == NULL);
		dataPages[index] = new DataPage;
		dataPages[index]->SetOffset(DATAPAGE_OFFSET(index));
		dataPages[index]->SetPageSize(dataPageSize);
		numDataPages++;
		indexPage.Add(key, index, true); // TODO
		MarkPageDirty(&indexPage);
	}
	
	dataPages[index]->Set(key, value, copy);
	MarkPageDirty(dataPages[index]);
	
	// update index:
	rb = dataPages[index]->FirstKey();
	if (ReadBuffer::LessThan(key, rb))
	{
		indexPage.Update(key, index, copy);
		MarkPageDirty(&indexPage);
	}
	
	if (dataPages[index]->IsOverflowing())
		SplitDataPage(index);
	
	return true;
}

void File::Delete(ReadBuffer& key)
{
	int32_t index;
	
	index = Locate(key);
	
	if (index < 0)
		return;
	
	dataPages[index]->Delete(key);
	MarkPageDirty(dataPages[index]);

	if (dataPages[index]->IsEmpty())
	{
		indexPage.Remove(dataPages[index]->FirstKey());
		MarkPageDirty(&indexPage);
	}
}

ReadBuffer File::FirstKey()
{
	return indexPage.FirstKey();
}

bool File::IsOverflowing()
{
	return isOverflowing || indexPage.IsOverflowing();
}

File* File::SplitFile()
{
	File*		newFile;
	uint32_t	index, newIndex, num;
	
	assert(numDataPageSlots == numDataPages);
	
	newFile = new File;
	newFile->indexPageSize = indexPageSize;
	newFile->dataPageSize = dataPageSize;
	newFile->numDataPageSlots = numDataPageSlots;
	newFile->indexPage.SetPageSize(indexPageSize);
	newFile->indexPage.SetNumDataPageSlots(numDataPageSlots);
	
	ReorderPages();
	
	num = numDataPages;
	for (index = numDataPageSlots / 2, newIndex = 0; index < num; index++, newIndex++)
	{
		newFile->dataPages[newIndex] = dataPages[index];
		dataPages[index] = NULL;
		newFile->dataPages[newIndex]->SetOffset(DATAPAGE_OFFSET(newIndex));

		numDataPages--;
		newFile->numDataPages++;

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

void File::Read()
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
	dataPages = (DataPage**) malloc(sizeof(DataPage*) * numDataPageSlots);
	for (i = 0; i < numDataPageSlots; i++)
		dataPages[i] = NULL;
	numDataPages = indexPage.NumEntries();	
}

void File::ReadRest()
{
	KeyIndex*		it;

	// TODO make sure this IO occurs in order!
	for (it = indexPage.keys.First(); it != NULL; it = indexPage.keys.Next(it))
		if (dataPages[it->index] == NULL)
			LoadDataPage(it->index);
}

void File::Write()
{
	Buffer			buffer;
	Page*			it;
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
	}
	
	// TODO: write these in offset order
	for (it = dirtyPages.First(); it != NULL; it = dirtyPages.Remove(it))
	{
		buffer.Allocate(it->GetPageSize());
		buffer.Zero();
//		printf("writing at offset %u\n", it->GetOffset());
		it->Write(buffer);
		if (pwrite(fd, buffer.GetBuffer(), it->GetPageSize(), it->GetOffset()) < 0)
			ASSERT_FAIL();
		it->SetDirty(false);
	}
}

int32_t File::Locate(ReadBuffer& key)
{
	int32_t	index;
	
	index = indexPage.Locate(key);
	
	if (index < 0)
		return index; // not in file
	
	if (dataPages[index] == NULL)
		LoadDataPage(index);
	
	return index;
}

void File::LoadDataPage(uint32_t index)
{
	Buffer		buffer;
	ReadBuffer	readBuffer;
	int			length;
	
	dataPages[index] = new DataPage;
	dataPages[index]->SetOffset(DATAPAGE_OFFSET(index));
	dataPages[index]->SetPageSize(dataPageSize);
	
//	printf("loading data page at index %u\n", index);

	buffer.Allocate(dataPageSize);
//	printf("reading page %u from %u\n", index, DATAPAGE_OFFSET(index));
	length = pread(fd, buffer.GetBuffer(), dataPageSize, DATAPAGE_OFFSET(index));
	if (length < 0)
		ASSERT_FAIL();
	buffer.SetLength(length);
	readBuffer.Wrap(buffer);
	dataPages[index]->Read(readBuffer);
}

void File::MarkPageDirty(Page* page)
{
	if (!page->IsDirty())
	{
		page->SetDirty(true);
		dirtyPages.Append(page);
	}
}

void File::SplitDataPage(uint32_t index)
{
	uint32_t	newIndex;
	DataPage*	newPage;

	if (numDataPages < numDataPageSlots)
	{
		newPage = dataPages[index]->SplitDataPage();
		numDataPages++;
		newIndex = indexPage.NextFreeDataPage();
		newPage->SetOffset(DATAPAGE_OFFSET(newIndex));
		assert(dataPages[newIndex] == NULL);
		dataPages[newIndex] = newPage;
		indexPage.Add(newPage->FirstKey(), newIndex, true); // TODO
//			if (newPage->MustSplit())
//			{
//				if (numDataPages < numDataPageSlots)
//				{
//					newPage = newPage->Split();
//					assert(newPage->MustSplit() == false);
// TOOD
//					newIndex = indexPage.NextFreeDataPage();
//					indexPage.Add(newPage->FirstKey(), newIndex, true);
//				}
//				else
//					mustSplit = true;
//			}
	}
	else
		isOverflowing = true;
}

void File::ReorderPages()
{
	uint32_t		newIndex, oldIndex, i;
	KeyIndex*		it;
	DataPage**		newDataPages;
	
	newDataPages = (DataPage**) calloc(numDataPageSlots, sizeof(DataPage*));
	
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

void File::ReorderFile()
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
