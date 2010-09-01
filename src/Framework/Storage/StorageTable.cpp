#include "StorageTable.h"
#include "System/Common.h"
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>
#include <dirent.h>

#define		RECOVERY_MARKER		0 // special as it's an illegal fileIndex

static int KeyCmp(const ReadBuffer& a, const ReadBuffer& b)
{
	return ReadBuffer::Cmp(a, b);
}

static ReadBuffer& Key(FileIndex* fi)
{
	return fi->key;
}

StorageTable::~StorageTable()
{
	files.DeleteTree();
}

void StorageTable::Open(const char* name)
{
	struct stat st;

	nextFileIndex = 1;

	this->name.Write(name);
	this->name.NullTerminate();

	tocFilepath.Write(name);
	tocFilepath.NullTerminate();

	recoveryFilepath.Write(name);
	recoveryFilepath.Append(".recovery");
	recoveryFilepath.NullTerminate();

	tocFD = open(tocFilepath.GetBuffer(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
	if (tocFD < 0)
		ASSERT_FAIL();

	recoveryFD = open(recoveryFilepath.GetBuffer(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
	if (recoveryFD < 0)
		ASSERT_FAIL();

	fstat(recoveryFD, &st);
	if (st.st_size > 0)
	{
		PerformRecovery(st.st_size);
		return;
	}
	
	fstat(tocFD, &st);
	if (st.st_size > 0)
		ReadTOC(st.st_size);
	prevCommitFileIndex = nextFileIndex;
}

void StorageTable::Commit(bool flush)
{
	WriteRecoveryPrefix();

	// to make sure the recovery (prefix) part is written
	if (flush)
		sync();

	WriteTOC();
	WriteData();

	// to make sure the data is written before we mark it such in the recovery postfix
	if (flush)
		sync();

	WriteRecoveryPostfix();
	
	lseek(recoveryFD, 0, SEEK_SET);
	ftruncate(recoveryFD, 0);

	prevCommitFileIndex = nextFileIndex;
}

void StorageTable::Close()
{
	FileIndex*	it;
	
	for (it = files.First(); it != NULL; it = files.Next(it))
	{
		if (it->file == NULL)
			continue;
		it->file->Close();
	}
	
	files.DeleteTree();
	close(recoveryFD);
	close(tocFD);
	
	::Delete(recoveryFilepath.GetBuffer());
}

const char*	StorageTable::GetName()
{
	return name.GetBuffer();
}

bool StorageTable::Get(ReadBuffer& key, ReadBuffer& value)
{
	FileIndex* fi;
	
	fi = Locate(key);

	if (fi == NULL)
		return false;
	
	else return fi->file->Get(key, value);
}

bool StorageTable::Set(ReadBuffer& key, ReadBuffer& value, bool copy)
{
	FileIndex	*fi;
	
	fi = Locate(key);

	if (fi == NULL)
	{
		fi = new FileIndex;
		fi->index = nextFileIndex++;
		fi->file = new StorageFile;
		fi->file->SetFileIndex(fi->index);
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

void StorageTable::Delete(ReadBuffer& key)
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

void StorageTable::WritePath(Buffer& buffer, uint32_t index)
{
	char	buf[30];
	
	snprintf(buf, sizeof(buf), ".%010u", index);
	
	buffer.Write(tocFilepath);
	buffer.SetLength(tocFilepath.GetLength() - 1); // get rid of trailing \0
	buffer.Append(buf);
	buffer.NullTerminate();
}

void StorageTable::ReadTOC(uint32_t length)
{
	uint32_t	i, numFiles;
	unsigned	len;
	char*		p;
	FileIndex*	fi;
	
	buffer.Allocate(length);
	if (read(tocFD, (void*) buffer.GetBuffer(), length) < 0)
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

void StorageTable::PerformRecovery(uint32_t length)
{
	char*				p;
	uint32_t			required, pageSize, marker;
	InList<Buffer>		pages;
	Buffer*				page;
	required = 0;
	
	while (true)
	{
		required += 4;
		if (length < required)
			goto TruncateLog;
		if (read(recoveryFD, (void*) buffer.GetBuffer(), 4) < 0)
			ASSERT_FAIL();
		p = buffer.GetBuffer();
		marker = FromLittle32(*((uint32_t*) p));
		
		if (marker == RECOVERY_MARKER)
			break;

		// it's a page
		pageSize = marker;
		required += (pageSize - 4);
		if (length < required)
			goto TruncateLog;

		buffer.SetLength(4);
		buffer.Allocate(pageSize);
		// TODO: return value
		if (read(recoveryFD, (void*) (buffer.GetBuffer() + 4), pageSize - 4) < 0)
			ASSERT_FAIL();

		buffer.SetLength(pageSize);
		page = new Buffer;
		page->Write(buffer);
		pages.Append(page);
	}

	// first marker was hit
	// read prevCommitFileIndex
	
	required += 4;	
	if (length < required)
		goto TruncateLog;

	if (read(recoveryFD, (void*) buffer.GetBuffer(), 4) < 0)
		ASSERT_FAIL();
	p = buffer.GetBuffer();
	prevCommitFileIndex = FromLittle32(*((uint32_t*) p));

	required += 4;	
	if (length < required)
		goto TruncateLog;
	if (read(recoveryFD, (void*) buffer.GetBuffer(), 4) < 0)
		ASSERT_FAIL();
	p = buffer.GetBuffer();
	marker = FromLittle32(*((uint32_t*) p));
	if (marker != RECOVERY_MARKER)
		goto TruncateLog;

	// done reading prefix part

	required += 4;	
	if (length < required)
	{
		WriteBackPages(pages);
		DeleteGarbageFiles();
		RebuildTOC();
		goto TruncateLog;
	}
	if (read(recoveryFD, (void*) buffer.GetBuffer(), 4) < 0)
		ASSERT_FAIL();
	p = buffer.GetBuffer();
	marker = FromLittle32(*((uint32_t*) p));
	if (marker != RECOVERY_MARKER)
	{
		WriteBackPages(pages);
		DeleteGarbageFiles();
		RebuildTOC();
		goto TruncateLog;
	}

TruncateLog:
	for (page = pages.First(); page != NULL; page = pages.Delete(page));
	lseek(recoveryFD, 0, SEEK_SET);
	ftruncate(recoveryFD, 0);	
	sync();	
	return;
}

void StorageTable::WriteBackPages(InList<Buffer>& pages)
{
	char*		p;
	int			fd;
	uint32_t	pageSize, fileIndex, offset;
	Buffer		filepath;
	Buffer*		page;
	
	for (page = pages.First(); page != NULL; page = pages.Next(page))
	{
		// parse the header part of page and write it

		p = page->GetBuffer();
		pageSize = FromLittle32(*((uint32_t*) p));
		p += 4;
		fileIndex = FromLittle32(*((uint32_t*) p));
		assert(fileIndex != 0);
		p += 4;
		offset = FromLittle32(*((uint32_t*) p));
		p += 4;
		WritePath(filepath, fileIndex);
		filepath.NullTerminate();
		fd = open(filepath.GetBuffer(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
		pwrite(fd, page->GetBuffer(), pageSize, offset);
		close(fd);
	}
}

void StorageTable::DeleteGarbageFiles()
{
	char*			p;
	DIR*			dir;
	struct dirent*	dirent;
	Buffer			buffer;
	unsigned		len, nread;
	uint32_t		index;
	ReadBuffer		firstKey;
	
	dir = opendir(".");
	
	while ((dirent = readdir(dir)) != NULL)
	{
		len = tocFilepath.GetLength() - 1;
		buffer.Write(dirent->d_name);
		if (buffer.GetLength() != len + 11)
			continue;
		if (strncmp(tocFilepath.GetBuffer(), buffer.GetBuffer(), len) != 0)
			continue;
		p = buffer.GetBuffer();
		if ((p + len)[0] != '.')
			continue;
		p += len + 1;
		len = buffer.GetLength() - len - 1;
		index = (uint32_t) BufferToUInt64(p, len, &nread);
		if (nread != len)
			continue;
		if (index >= prevCommitFileIndex)
		{
			::Delete(dirent->d_name);
		}
	}
	closedir(dir);
}

void StorageTable::RebuildTOC()
{
	char*			p;
	DIR*			dir;
	struct dirent*	dirent;
	Buffer			buffer;
	unsigned		len, nread;
	uint32_t		index;
	FileIndex*	fi;
	ReadBuffer		firstKey;
	
	dir = opendir(".");
	
	while ((dirent = readdir(dir)) != NULL)
	{
		buffer.Write(dirent->d_name);
		if (buffer.GetLength() != tocFilepath.GetLength() + 11)
			continue;
		if (strncmp(tocFilepath.GetBuffer(), buffer.GetBuffer(), tocFilepath.GetLength()) != 0)
			continue;
		p = buffer.GetBuffer();
		len = tocFilepath.GetLength();
		if ((p + len)[0] != '.')
			continue;
		p += len + 1;
		len = buffer.GetLength() - len;
		index = (uint32_t) BufferToUInt64(p, len, &nread);
		if (nread != len)
			continue;
		fi = new FileIndex;
		fi->file = new StorageFile;
		fi->file->Open(dirent->d_name);
		if (fi->file->IsEmpty())
		{
			::Delete(dirent->d_name);
			continue;
		}
		fi->SetKey(fi->file->FirstKey(), true);
		fi->file->Close();
		delete fi->file;
		fi->file = NULL;
	}
	
	closedir(dir);

	WriteTOC();	
}

void StorageTable::WriteRecoveryPrefix()
{
	FileIndex		*it;
	uint32_t		marker = RECOVERY_MARKER;
	
	for (it = files.First(); it != NULL; it = files.Next(it))
	{
		if (it->file == NULL)
			continue;
		it->file->WriteRecovery(recoveryFD); // only dirty old data pages' buffer is written
	}
	
	if (write(recoveryFD, (const void *) &marker, 4) < 0)
	{
		Log_Errno();
		ASSERT_FAIL();
	}
	
	if (write(recoveryFD, (const void *) &prevCommitFileIndex, 4) < 0)
	{
		Log_Errno();
		ASSERT_FAIL();
	}

	if (write(recoveryFD, (const void *) &marker, 4) < 0)
	{
		Log_Errno();
		ASSERT_FAIL();
	}
}

void StorageTable::WriteRecoveryPostfix()
{
	uint32_t		marker = RECOVERY_MARKER;

	if (write(recoveryFD, (const void *) &marker, 4) < 0)
		ASSERT_FAIL();
}

void StorageTable::WriteTOC()
{
	char*			p;
	uint32_t		size, len;
	uint32_t		tmp;
	FileIndex		*it;

	lseek(tocFD, 0, SEEK_SET);
	ftruncate(tocFD, 0);
	
	len = files.GetCount();
	tmp = ToLittle32(len);

	if (write(tocFD, (const void *) &tmp, 4) < 0)
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
		if (write(tocFD, (const void *) buffer.GetBuffer(), size) < 0)
			ASSERT_FAIL();
	}
}

void StorageTable::WriteData()
{
	FileIndex		*it;
	
	for (it = files.First(); it != NULL; it = files.Next(it))
	{
		if (it->file == NULL)
			continue;
		it->file->WriteData(); // only changed data pages are written
	}
}

FileIndex* StorageTable::Locate(ReadBuffer& key)
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
		fi->file->SetFileIndex(fi->index);
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

void FileIndex::SetKey(ReadBuffer key_, bool copy)
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

void StorageTable::SplitFile(StorageFile* file)
{
	FileIndex*	newFi;
	ReadBuffer	rb;

	file->ReadRest();
	newFi = new FileIndex;
	newFi->file = file->SplitFile();
	newFi->index = nextFileIndex++;
	newFi->file->SetFileIndex(newFi->index);
	
	WritePath(newFi->filepath, newFi->index);
	newFi->file->Open(newFi->filepath.GetBuffer());

	rb = newFi->file->FirstKey();
	newFi->SetKey(rb, true); // TODO: buffer management
	files.Insert(newFi);
}

StorageDataPage* StorageTable::CursorBegin(ReadBuffer& key, Buffer& nextKey)
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

void StorageTable::CommitPhase1()
{
	WriteRecoveryPrefix();
}

void StorageTable::CommitPhase2()
{
	WriteTOC();
	WriteData();
}

void StorageTable::CommitPhase3()
{
	WriteRecoveryPostfix();
	
	lseek(recoveryFD, 0, SEEK_SET);
	ftruncate(recoveryFD, 0);

	prevCommitFileIndex = nextFileIndex;
}
