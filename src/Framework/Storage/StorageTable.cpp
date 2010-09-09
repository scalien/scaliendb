#include "StorageTable.h"
#include "StorageFileHeader.h"
#include "System/FileSystem.h"

#define LAST_SHARD_KEY		"\377\377\377\377\377\377\377\377"
#define FILE_TYPE			"ScalienDB table index"
#define FILE_VERSION_MAJOR	0
#define FILE_VERSION_MINOR	1

static int KeyCmp(const ReadBuffer& a, const ReadBuffer& b)
{
	return ReadBuffer::Cmp(a, b);
}

static const ReadBuffer& Key(StorageShardIndex* si)
{
	return si->startKey;
}

const char* StorageTable::GetName()
{
	return name.GetBuffer();
}

uint64_t StorageTable::GetSize()
{
	StorageShardIndex*	si;
	uint64_t			size;
	
	size = 0;
	for (si = shards.First(); si != NULL; si = shards.Next(si))
		size += si->shard->GetSize();
	
	return size;
}

void StorageTable::Open(const char* dir, const char* name_)
{
//	int64_t	recoverySize;
	int64_t	tocSize;
	char	sep;
	
	// create table directory
	if (*dir == '\0')
		path.Append(".");
	else
		path.Append(dir);

	sep = FS_Separator();
	if (path.GetBuffer()[path.GetLength() - 1] != sep)
		path.Append(&sep, 1);
	
	path.Append(name_);
	path.Append(&sep, 1);
	path.NullTerminate();
	if (!FS_IsDirectory(path.GetBuffer()))
	{
		if (!FS_CreateDir(path.GetBuffer()))
			ASSERT_FAIL();
	}
	
	name.Write(name_);
	name.NullTerminate();

	tocFilepath.Write(path.GetBuffer(), path.GetLength() - 1);
	tocFilepath.Append("shards");
	tocFilepath.NullTerminate();

	// TODO: recovery
//	recoveryFilepath.Write(name);
//	recoveryFilepath.Append(".recovery");
//	recoveryFilepath.NullTerminate();

	tocFD = FS_Open(tocFilepath.GetBuffer(), FS_READWRITE | FS_CREATE);
	if (tocFD == INVALID_FD)
		ASSERT_FAIL();

	// TODO: recovery
//	recoveryFD = FS_Open(recoveryFilepath.GetBuffer(), FS_READWRITE | FS_CREATE);
//	if (recoveryFD == INVALID_FD)
//		ASSERT_FAIL();
//
//	recoverySize = FS_FileSize(recoveryFD);
//	if (recoverySize > 0)
//	{
//		PerformRecovery(recoverySize);
//		return;
//	}
	
	tocSize = FS_FileSize(tocFD);
	if (tocSize > 0)
		ReadTOC(tocSize);

	// create default shard if not exists
	if (shards.GetCount() == 0)
	{
		ReadBuffer	startKey;
		ReadBuffer	endKey;
		
		CreateShard(0, startKey, endKey);
	}

}

void StorageTable::Commit(bool recovery, bool flush)
{
	if (recovery)
	{
		CommitPhase1();
		if (flush)
			FS_Sync();
	}

	CommitPhase2();
	if (flush)
		FS_Sync();

	if (recovery)
		CommitPhase3();
	
	CommitPhase4();
}

void StorageTable::Close()
{
	StorageShardIndex*	it;
	
	for (it = shards.First(); it != NULL; it = shards.Next(it))
	{
		if (it->shard == NULL)
			continue;
		it->shard->Close();
	}
	
	shards.DeleteTree();

	FS_FileClose(tocFD);
	tocFD = INVALID_FD;
}

bool StorageTable::Get(ReadBuffer& key, ReadBuffer& value)
{
	StorageShardIndex* si;
	
	si = Locate(key);

	if (si == NULL)
		return false;
	
	return si->shard->Get(key, value);
}

bool StorageTable::Set(ReadBuffer& key, ReadBuffer& value, bool copy)
{
	StorageShardIndex	*si;
	
	si = Locate(key);

	if (si == NULL)
		return false;
	
	if (!si->shard->Set(key, value, copy))
		return false;

	return true;
}

void StorageTable::Delete(ReadBuffer& key)
{
	StorageShardIndex*	si;
	ReadBuffer			firstKey;
	
	si = Locate(key);

	if (si == NULL)
		return;
	
	si->shard->Delete(key);
}

bool StorageTable::CreateShard(uint64_t shardID, ReadBuffer& startKey_, ReadBuffer& endKey_)
{
	ReadBuffer			endKey;
	StorageShardIndex*	si;

	if (endKey_.GetLength() == 0)
		endKey.Set((char*) LAST_SHARD_KEY, sizeof(LAST_SHARD_KEY) - 1);
	else
		endKey = endKey_;
	
	// TODO: check if the shard or the key interval already exists
	si = new StorageShardIndex;
	si->SetStartKey(startKey_, true);
	si->SetEndKey(endKey, true);
	si->shardID = shardID;
	shards.Insert(si);
	
	return true;
}

bool StorageTable::SplitShard(uint64_t oldShardID, uint64_t newShardID, ReadBuffer& startKey)
{
	// TODO:
	return true;
}

StorageShardIndex* StorageTable::Locate(ReadBuffer& key)
{
	StorageShardIndex*	si;
	int					cmpres;
	
	if (shards.GetCount() == 0)
		return NULL;
	
	if (ReadBuffer::LessThan(key, shards.First()->startKey))
	{
		ASSERT_FAIL();
		return NULL;
	}
	
	if (ReadBuffer::Cmp(key, shards.Last()->endKey) > 0)
	{
		ASSERT_FAIL();
		return NULL;
	}
	
	si = shards.Locate<ReadBuffer&>(key, cmpres);
	if (si)
	{
		if (cmpres < 0)
			si = shards.Prev(si);
	}
	else
		si = shards.Last();
	
	if (si->shard == NULL)
	{
		Buffer	dir;
		
		dir.Write(path.GetBuffer(), path.GetLength() - 1);
		dir.Appendf("%U", si->shardID);
		dir.NullTerminate();
		
		si->shard = new StorageShard;
		si->shard->Open(dir.GetBuffer(), name.GetBuffer());
	}
	
	return si;
}

void StorageTable::ReadTOC(uint32_t length)
{
	uint32_t			i, numShards;
	unsigned			len;
	char*				p;
	StorageShardIndex*	si;
	int					ret;
	StorageFileHeader	header;
	Buffer				headerBuf;
	
	headerBuf.Allocate(STORAGEFILE_HEADER_LENGTH);
	if ((ret = FS_FileRead(tocFD, (void*) headerBuf.GetBuffer(), STORAGEFILE_HEADER_LENGTH)) < 0)
		ASSERT_FAIL();
	if (ret != STORAGEFILE_HEADER_LENGTH)
		ASSERT_FAIL();
	headerBuf.SetLength(STORAGEFILE_HEADER_LENGTH);
	if (!header.Read(headerBuf))
		ASSERT_FAIL();
	
	length -= STORAGEFILE_HEADER_LENGTH;
	if ((ret = FS_FileRead(tocFD, (void*) buffer.GetBuffer(), length)) < 0)
		ASSERT_FAIL();
	p = buffer.GetBuffer();
	numShards = FromLittle32(*((uint32_t*) p));
	assert(numShards * 8 + 4 <= length);
	p += 4;
	for (i = 0; i < numShards; i++)
	{
		si = new StorageShardIndex;
		si->shardID = FromLittle32(*((uint32_t*) p));
		p += 4;
		len = FromLittle32(*((uint32_t*) p));
		p += 4;
		si->startKey.SetLength(len);
		si->startKey.SetBuffer(p);
//		si->SetStartKey(ReadBuffer(p, len), true);
		p += len;
		len = FromLittle32(*((uint32_t*) p));
		p += 4;
		si->endKey.SetLength(len);
		si->endKey.SetBuffer(p);
//		si->SetEndKey(ReadBuffer(p, len), true);
		p += len;
		shards.Insert(si);
	}
}

void StorageTable::WriteTOC()
{
	char*				p;
	uint32_t			size, len;
	uint32_t			tmp;
	StorageShardIndex	*it;
	StorageFileHeader	header;
	Buffer				writeBuf;

	FS_FileSeek(tocFD, 0, FS_SEEK_SET);
	FS_FileTruncate(tocFD, 0);
	
	header.Init(FILE_TYPE, FILE_VERSION_MAJOR, FILE_VERSION_MINOR, 0);
	header.Write(writeBuf);

	if (FS_FileWrite(tocFD, (const void*) writeBuf.GetBuffer(), writeBuf.GetLength()) < 0)
		ASSERT_FAIL();
	
	len = shards.GetCount();
	tmp = ToLittle32(len);

	if (FS_FileWrite(tocFD, (const void *) &tmp, 4) < 0)
		ASSERT_FAIL();

	for (it = shards.First(); it != NULL; it = shards.Next(it))
	{		
		size = 4 + (4+it->startKey.GetLength()) + (4+it->endKey.GetLength());
		writeBuf.Allocate(size);
		p = writeBuf.GetBuffer();
		*((uint32_t*) p) = ToLittle32(it->shardID);
		p += 4;
		len = it->startKey.GetLength();
		*((uint32_t*) p) = ToLittle32(len);
		p += 4;
		memcpy(p, it->startKey.GetBuffer(), len);
		p += len;
		len = it->endKey.GetLength();
		*((uint32_t*) p) = ToLittle32(len);
		p += 4;
		memcpy(p, it->endKey.GetBuffer(), len);
		p += len;
		if (FS_FileWrite(tocFD, (const void *) writeBuf.GetBuffer(), size) < 0)
			ASSERT_FAIL();
	}
}

void StorageTable::CommitPhase1()
{
	StorageShardIndex*	si;
	
	for (si = shards.First(); si != NULL; si = shards.Next(si))
		si->shard->CommitPhase1();
}

void StorageTable::CommitPhase2()
{
	StorageShardIndex*	si;
	
	// TODO: recovery
	WriteTOC();

	for (si = shards.First(); si != NULL; si = shards.Next(si))
		si->shard->CommitPhase2();
}

void StorageTable::CommitPhase3()
{
	StorageShardIndex*	si;
	
	for (si = shards.First(); si != NULL; si = shards.Next(si))
		si->shard->CommitPhase3();
}

void StorageTable::CommitPhase4()
{
	StorageShardIndex*	si;
	
	for (si = shards.First(); si != NULL; si = shards.Next(si))
		si->shard->CommitPhase4();
}


