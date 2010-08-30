#include "StorageCatalog.h"
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>

static int KeyCmp(const ReadBuffer& a, const ReadBuffer& b)
{
	return ReadBuffer::Cmp(a, b);
}

static ReadBuffer& Key(FileIndex* fi)
{
	return fi->key;
}

StorageCatalog::~StorageCatalog()
{
	files.DeleteTree();
}

void StorageCatalog::Open(const char* filepath_)
{
	struct stat st;

	nextFileIndex = 0;

	filepath.Write(filepath_);
	filepath.NullTerminate();

	fd = open(filepath.GetBuffer(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);

	if (fd < 0)
		ASSERT_FAIL();

	fstat(fd, &st);
	if (st.st_size > 0)
		Read(st.st_size);
}

void StorageCatalog::Flush()
{
	Write(false);
	sync();
}

void StorageCatalog::Close()
{
	FileIndex*	it;
	
	Write(true);

	for (it = files.First(); it != NULL; it = files.Next(it))
	{
		if (it->file == NULL)
			continue;
		it->file->Close();
	}
	
	close(fd);
}

bool StorageCatalog::Get(ReadBuffer& key, ReadBuffer& value)
{
	FileIndex* fi;
	
	fi = Locate(key);

	if (fi == NULL)
		return false;
	
	else return fi->file->Get(key, value);
}

bool StorageCatalog::Set(ReadBuffer& key, ReadBuffer& value, bool copy)
{
	FileIndex	*fi;
	
	fi = Locate(key);

	if (fi == NULL)
	{
		fi = new FileIndex;
		fi->index = nextFileIndex++;
		fi->file = new StorageFile;
		WritePath(fi->filepath, fi->index);
		fi->file->Open(fi->filepath.GetBuffer());
		fi->SetKey(key, true); // TODO: buffer management
		files.Insert(fi);
	}
	
	if (!fi->file->Set(key, value, copy))
		return false;

	// update index:
	if (ReadBuffer::LessThan(key, fi->key))
		fi->SetKey(key, true);

	if (fi->file->IsOverflowing())
		SplitFile(fi->file);
	
	return true;
}

void StorageCatalog::Delete(ReadBuffer& key)
{
	bool			updateIndex;
	FileIndex*		fi;
	ReadBuffer		firstKey;
	
	fi = Locate(key);

	if (fi == NULL)
		return;
	
	updateIndex = false;
	firstKey = fi->file->FirstKey();
	if (BUFCMP(&key, &firstKey))
		updateIndex = true;
	
	fi->file->Delete(key);
	
	if (fi->file->IsEmpty())
	{
		fi->file->Close();
		unlink(fi->filepath.GetBuffer());
		files.Remove(fi);
		delete fi;
	}
	else if (updateIndex)
	{
		firstKey = fi->file->FirstKey();
		fi->SetKey(firstKey, true);
	}
}

void StorageCatalog::WritePath(Buffer& buffer, uint32_t index)
{
	char	buf[30];
	
	snprintf(buf, sizeof(buf), ".%010u", index);
	
	buffer.Write(filepath);
	buffer.SetLength(filepath.GetLength() - 1); // get rid of trailing \0
	buffer.Append(buf);
	buffer.NullTerminate();
}

void StorageCatalog::Read(uint32_t length)
{
	uint32_t	i, numFiles;
	unsigned	len;
	char*		p;
	FileIndex*	fi;
	
	buffer.Allocate(length);
	if (read(fd, (void*) buffer.GetBuffer(), length) < 0)
		ASSERT_FAIL();
	p = buffer.GetBuffer();
	numFiles = FromLittle32(*((uint32_t*) p));
	assert(numFiles * 8 + 4 <= length);
	p += 4;
	for (i = 0; i < numFiles; i++)
	{
		fi = new FileIndex;
		fi->index = FromLittle32(*((uint32_t*) p));
		p += 4;
		len = FromLittle32(*((uint32_t*) p));
		p += 4;
		fi->key.SetLength(len);
		fi->key.SetBuffer(p);
		p += len;
		files.Insert(fi);
		WritePath(fi->filepath, fi->index);
		if (fi->index + 1 > nextFileIndex)
			nextFileIndex = fi->index + 1;
	}	
}

void StorageCatalog::Write(bool flush)
{
	char*			p;
	uint32_t		size, len;
	FileIndex		*it;
	
	lseek(fd, 0, SEEK_SET);
	ftruncate(fd, 0);
	
	buffer.Allocate(4);
	p = buffer.GetBuffer();
	len = files.GetCount();
	*((uint32_t*) p) = ToLittle32(len);
	p += 4;
	
	if (write(fd, (const void *) buffer.GetBuffer(), 4) < 0)
		ASSERT_FAIL();

	for (it = files.First(); it != NULL; it = files.Next(it))
	{		
		size = 8 + it->key.GetLength();
		buffer.Allocate(size);
		p = buffer.GetBuffer();
		*((uint32_t*) p) = ToLittle32(it->index);
		p += 4;
		len = it->key.GetLength();
		*((uint32_t*) p) = ToLittle32(len);
		p += 4;
		memcpy(p, it->key.GetBuffer(), len);
		p += len;
		if (write(fd, (const void *) buffer.GetBuffer(), size) < 0)
			ASSERT_FAIL();
	}

	for (it = files.First(); it != NULL; it = files.Next(it))
	{
		if (it->file == NULL)
			continue;
		it->file->Write(flush); // only changed data pages are written
	}
}

FileIndex* StorageCatalog::Locate(ReadBuffer& key)
{
	FileIndex*	fi;
	int			cmpres;
	
	if (files.GetCount() == 0)
		return NULL;
		
	if (ReadBuffer::LessThan(key, files.First()->key))
	{
		fi = files.First();
		goto OpenFile;
	}
	
//	for (fi = files.First(); fi != NULL; fi = files.Next(fi))
//	{
//		if (ReadBuffer::LessThan(key, fi->key))
//		{
//			fi = files.Prev(fi);
//			goto OpenFile;
//		}
//	}
	
	fi = files.Locate<ReadBuffer&>(key, cmpres);
	if (fi)
	{
		if (cmpres < 0)
			fi = files.Prev(fi);
	}
	else
		fi = files.Last();
	
OpenFile:
	if (fi->file == NULL)
	{
		fi->file = new StorageFile;
		fi->file->Open(fi->filepath.GetBuffer());
	}
	
	return fi;
}

FileIndex::FileIndex()
{
	file = NULL;
	keyBuffer = NULL;
}

FileIndex::~FileIndex()
{
	if (keyBuffer != NULL)
		delete keyBuffer;
	delete file;
}

void FileIndex::SetKey(ReadBuffer& key_, bool copy)
{
	if (keyBuffer != NULL && !copy)
		delete keyBuffer;
	
	if (copy)
	{
		if (keyBuffer == NULL)
			keyBuffer = new Buffer; // TODO: allocation strategy
		keyBuffer->Write(key_);
		key.Wrap(*keyBuffer);
	}
	else
		key = key_;
}

void StorageCatalog::SplitFile(StorageFile* file)
{
	FileIndex*	newFi;
	ReadBuffer	rb;

	file->ReadRest();
	newFi = new FileIndex;
	newFi->file = file->SplitFile();
	newFi->index = nextFileIndex++;
	
	WritePath(newFi->filepath, newFi->index);
	newFi->file->Open(newFi->filepath.GetBuffer());

	rb = newFi->file->FirstKey();
	newFi->SetKey(rb, true); // TODO: buffer management
	files.Insert(newFi);
}

StorageDataPage* StorageCatalog::CursorBegin(ReadBuffer& key, Buffer& nextKey)
{
	FileIndex*			fi;
	StorageDataPage*	dataPage;
	
	fi = Locate(key);

	if (fi == NULL)
		return NULL;
	
	dataPage = fi->file->CursorBegin(key, nextKey);
	if (nextKey.GetLength() != 0)
		return dataPage;
	
	fi = files.Next(fi);
	if (fi != NULL)
		nextKey.Write(fi->key);

	return dataPage;
}