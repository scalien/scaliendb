#include "StorageTable.h"
#include "System/Common.h"
#include "System/FileSystem.h"

#include <stdio.h>

#define	RECOVERY_MARKER		0		// special as it's an illegal fileIndex

static int KeyCmp(const ReadBuffer& a, const ReadBuffer& b)
{
	return ReadBuffer::Cmp(a, b);
}

static ReadBuffer& Key(StorageFileIndex* fi)
{
	return fi->key;
}

StorageTable::~StorageTable()
{
	files.DeleteTree();
}

void StorageTable::Open(const char* name)
{
	int64_t	recoverySize;
	int64_t	tocSize;
	
	nextStorageFileIndex = 1;

	this->name.Write(name);
	this->name.NullTerminate();

	tocFilepath.Write(name);
	tocFilepath.NullTerminate();

	recoveryFilepath.Write(name);
	recoveryFilepath.Append(".recovery");
	recoveryFilepath.NullTerminate();

	tocFD = FS_Open(tocFilepath.GetBuffer(), FS_READWRITE | FS_CREATE);
	if (tocFD < 0)
		ASSERT_FAIL();

	recoveryFD = FS_Open(recoveryFilepath.GetBuffer(), FS_READWRITE | FS_CREATE);
	if (recoveryFD < 0)
		ASSERT_FAIL();

	recoverySize = FS_FileSize(recoveryFD);
	if (recoverySize > 0)
	{
		PerformRecovery(recoverySize);
		return;
	}
	
	tocSize = FS_FileSize(tocFD);
	if (tocSize > 0)
		ReadTOC(tocSize);
	prevCommitStorageFileIndex = nextStorageFileIndex;
}

void StorageTable::Commit(bool recovery, bool flush)
{
	if (recovery)
	{
		WriteRecoveryPrefix();

		// to make sure the recovery (prefix) part is written
		if (flush)
			FS_Sync();
	}

	WriteTOC();
	WriteData();

	// to make sure the data is written before we mark it such in the recovery postfix
	if (flush)
		FS_Sync();

	if (recovery)
	{
		WriteRecoveryPostfix();
		
		FS_FileSeek(recoveryFD, 0, FS_SEEK_SET);
		FS_FileTruncate(recoveryFD, 0);
	}
	prevCommitStorageFileIndex = nextStorageFileIndex;
}

void StorageTable::Close()
{
	StorageFileIndex*	it;
	
	for (it = files.First(); it != NULL; it = files.Next(it))
	{
		if (it->file == NULL)
			continue;
		it->file->Close();
	}
	
	files.DeleteTree();
	FS_FileClose(recoveryFD);
	FS_FileClose(tocFD);
	
	FS_Delete(recoveryFilepath.GetBuffer());
}

const char*	StorageTable::GetName()
{
	return name.GetBuffer();
}

bool StorageTable::Get(ReadBuffer& key, ReadBuffer& value)
{
	StorageFileIndex* fi;
	
	fi = Locate(key);

	if (fi == NULL)
		return false;
	
	else return fi->file->Get(key, value);
}

bool StorageTable::Set(ReadBuffer& key, ReadBuffer& value, bool copy)
{
	StorageFileIndex	*fi;
	
	fi = Locate(key);

	if (fi == NULL)
	{
		fi = new StorageFileIndex;
		fi->index = nextStorageFileIndex++;
		fi->file = new StorageFile;
		fi->file->SetStorageFileIndex(fi->index);
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
	StorageFileIndex*		fi;
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
		FS_Delete(fi->filepath.GetBuffer());
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
	StorageFileIndex*	fi;
	
	buffer.Allocate(length);
	if (FS_FileRead(tocFD, (void*) buffer.GetBuffer(), length) < 0)
		ASSERT_FAIL();
	p = buffer.GetBuffer();
	numFiles = FromLittle32(*((uint32_t*) p));
	assert(numFiles * 8 + 4 <= length);
	p += 4;
	for (i = 0; i < numFiles; i++)
	{
		fi = new StorageFileIndex;
		fi->index = FromLittle32(*((uint32_t*) p));
		p += 4;
		len = FromLittle32(*((uint32_t*) p));
		p += 4;
		fi->key.SetLength(len);
		fi->key.SetBuffer(p);
		p += len;
		files.Insert(fi);
		WritePath(fi->filepath, fi->index);
		if (fi->index + 1 > nextStorageFileIndex)
			nextStorageFileIndex = fi->index + 1;
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
		if (FS_FileRead(recoveryFD, (void*) buffer.GetBuffer(), 4) < 0)
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
		if (FS_FileRead(recoveryFD, (void*) (buffer.GetBuffer() + 4), pageSize - 4) < 0)
			ASSERT_FAIL();

		buffer.SetLength(pageSize);
		page = new Buffer;
		page->Write(buffer);
		pages.Append(page);
	}

	// first marker was hit
	// read prevCommitStorageFileIndex
	
	required += 4;	
	if (length < required)
		goto TruncateLog;

	if (FS_FileRead(recoveryFD, (void*) buffer.GetBuffer(), 4) < 0)
		ASSERT_FAIL();
	p = buffer.GetBuffer();
	prevCommitStorageFileIndex = FromLittle32(*((uint32_t*) p));

	required += 4;	
	if (length < required)
		goto TruncateLog;
	if (FS_FileRead(recoveryFD, (void*) buffer.GetBuffer(), 4) < 0)
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
	if (FS_FileRead(recoveryFD, (void*) buffer.GetBuffer(), 4) < 0)
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
	FS_FileSeek(recoveryFD, 0, SEEK_SET);
	FS_FileTruncate(recoveryFD, 0);	
	FS_Sync();	
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
		fd = FS_Open(filepath.GetBuffer(), FS_READWRITE | FS_CREATE);
		FS_FileWriteOffs(fd, page->GetBuffer(), pageSize, offset);
		FS_FileClose(fd);
	}
}

void StorageTable::DeleteGarbageFiles()
{
	char*			p;
	FS_Dir			dir;
	FS_DirEntry		dirent;
	Buffer			buffer;
	unsigned		len, nread;
	uint32_t		index;
	ReadBuffer		firstKey;
	
	dir = FS_OpenDir(".");
	
	while ((dirent = FS_ReadDir(dir)) != FS_INVALID_DIR_ENTRY)
	{
		len = tocFilepath.GetLength() - 1;
		buffer.Write(FS_DirEntryName(dirent));
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
		if (index >= prevCommitStorageFileIndex)
			FS_Delete(FS_DirEntryName(dirent));
	}

	FS_CloseDir(dir);
}

void StorageTable::RebuildTOC()
{
	char*			p;
	FS_Dir			dir;
	FS_DirEntry		dirent;
	Buffer			buffer;
	unsigned		len, nread;
	uint32_t		index;
	StorageFileIndex*	fi;
	ReadBuffer		firstKey;
	
	dir = FS_OpenDir(".");
	
	while ((dirent = FS_ReadDir(dir)) != FS_INVALID_DIR_ENTRY)
	{
		buffer.Write(FS_DirEntryName(dirent));
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
		fi = new StorageFileIndex;
		fi->file = new StorageFile;
		fi->file->Open(FS_DirEntryName(dirent));
		if (fi->file->IsEmpty())
		{
			FS_Delete(FS_DirEntryName(dirent));
			continue;
		}
		fi->SetKey(fi->file->FirstKey(), true);
		fi->file->Close();
		delete fi->file;
		fi->file = NULL;
	}
	
	FS_CloseDir(dir);

	WriteTOC();	
}

void StorageTable::WriteRecoveryPrefix()
{
	StorageFileIndex		*it;
	uint32_t		marker = RECOVERY_MARKER;
	
	for (it = files.First(); it != NULL; it = files.Next(it))
	{
		if (it->file == NULL)
			continue;
		it->file->WriteRecovery(recoveryFD); // only dirty old data pages' buffer is written
	}
	
	if (FS_FileWrite(recoveryFD, (const void *) &marker, 4) < 0)
	{
		Log_Errno();
		ASSERT_FAIL();
	}
	
	if (FS_FileWrite(recoveryFD, (const void *) &prevCommitStorageFileIndex, 4) < 0)
	{
		Log_Errno();
		ASSERT_FAIL();
	}

	if (FS_FileWrite(recoveryFD, (const void *) &marker, 4) < 0)
	{
		Log_Errno();
		ASSERT_FAIL();
	}
}

void StorageTable::WriteRecoveryPostfix()
{
	uint32_t		marker = RECOVERY_MARKER;

	if (FS_FileWrite(recoveryFD, (const void *) &marker, 4) < 0)
		ASSERT_FAIL();
}

void StorageTable::WriteTOC()
{
	char*			p;
	uint32_t		size, len;
	uint32_t		tmp;
	StorageFileIndex		*it;

	lseek(tocFD, 0, SEEK_SET);
	ftruncate(tocFD, 0);
	
	len = files.GetCount();
	tmp = ToLittle32(len);

	if (FS_FileWrite(tocFD, (const void *) &tmp, 4) < 0)
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
		if (FS_FileWrite(tocFD, (const void *) buffer.GetBuffer(), size) < 0)
			ASSERT_FAIL();
	}
}

void StorageTable::WriteData()
{
	StorageFileIndex		*it;
	
	for (it = files.First(); it != NULL; it = files.Next(it))
	{
		if (it->file == NULL)
			continue;
		it->file->WriteData(); // only changed data pages are written
	}
}

StorageFileIndex* StorageTable::Locate(ReadBuffer& key)
{
	StorageFileIndex*	fi;
	int			cmpres;
	
	if (files.GetCount() == 0)
		return NULL;
		
	if (ReadBuffer::LessThan(key, files.First()->key))
	{
		fi = files.First();
		goto OpenFile;
	}
	
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
		fi->file->SetStorageFileIndex(fi->index);
	}
	
	return fi;
}

void StorageTable::SplitFile(StorageFile* file)
{
	StorageFileIndex*	newFi;
	ReadBuffer	rb;

	file->ReadRest();
	newFi = new StorageFileIndex;
	newFi->file = file->SplitFile();
	newFi->index = nextStorageFileIndex++;
	newFi->file->SetStorageFileIndex(newFi->index);
	
	WritePath(newFi->filepath, newFi->index);
	newFi->file->Open(newFi->filepath.GetBuffer());

	rb = newFi->file->FirstKey();
	newFi->SetKey(rb, true); // TODO: buffer management
	files.Insert(newFi);
}

StorageDataPage* StorageTable::CursorBegin(ReadBuffer& key, Buffer& nextKey)
{
	StorageFileIndex*			fi;
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
	
	FS_FileSeek(recoveryFD, 0, FS_SEEK_SET);
	FS_FileTruncate(recoveryFD, 0);

}

void StorageTable::CommitPhase4()
{
	prevCommitStorageFileIndex = nextStorageFileIndex;
}