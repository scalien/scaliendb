#include "File.h"
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdio.h> // REMOVE

#define DATAPAGE_OFFSET(idx) (INDEXPAGE_OFFSET+indexPageSize+idx*dataPageSize)

File::File()
{
	uint32_t i;
	
	indexPageSize = DEFAULT_INDEXPAGE_SIZE;
	dataPageSize = DEFAULT_DATAPAGE_SIZE;
	numDataPageSlots = DEFAULT_NUM_DATAPAGES;

	isOverflowing = false;
	
	indexPage.SetOffset(INDEXPAGE_OFFSET);
	indexPage.SetPageSize(indexPageSize);
	indexPage.SetNumDataPageSlots(numDataPageSlots);

	dataPages = (DataPage**) malloc(sizeof(DataPage*) * numDataPageSlots);
	for (i = 0; i < numDataPageSlots; i++)
		dataPages[i] = NULL;
	numDataPages = 0;
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
	// sync
}

void File::Close()
{
	Write();
	
	close(fd);
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
	int32_t		index, newIndex;
	DataPage*	newPage;
	
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
	
	if (dataPages[index]->IsOverflowing())
	{
		if (numDataPages < numDataPageSlots)
		{
			newPage = dataPages[index]->Split();
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
		indexPage.Remove(index);
		MarkPageDirty(&indexPage);
	}
}

bool File::IsOverflowing()
{
	return isOverflowing || indexPage.IsOverflowing();
}

void File::Read()
{
	char*			p;
	unsigned		i;
	int				length;
	Buffer			buffer;
	ReadBuffer		readBuffer;

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

void File::Write()
{
	Buffer			buffer;
	Page*			it;
	char*			p;

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

	// TODO: write (store) these in offset order
	for (it = dirtyPages.Head(); it != NULL; it = dirtyPages.Remove(it))
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
	
	printf("loading data page at index %u\n", index);

	buffer.Allocate(dataPageSize);
	printf("reading page %u from %u\n", index, DATAPAGE_OFFSET(index));
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
