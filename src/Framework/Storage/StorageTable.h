#ifndef STORAGETABLE_H
#define STORAGETABLE_H

#include "System/Containers/InTreeMap.h"
#include "StorageFile.h"
#include "StorageCursor.h"
/*
===============================================================================

 FileIndex

===============================================================================
*/

class FileIndex
{
public:
	FileIndex();
	~FileIndex();
	
	void					SetKey(ReadBuffer key, bool copy);

	Buffer					filepath;
	StorageFile*			file;

	ReadBuffer				key;
	Buffer*					keyBuffer;
	uint32_t				index;
	
	InTreeNode<FileIndex>	treeNode;
};

inline bool LessThan(FileIndex &a, FileIndex &b)
{
	return ReadBuffer::LessThan(a.key, b.key);
}

class StorageDatabase; // forward

/*
===============================================================================

 StorageTable

===============================================================================
*/

class StorageTable
{
public:
	~StorageTable();
	
	void					Open(const char* name);
	void					Commit(bool flush = true);
	void					Close();
	
	const char*				GetName();
	
	bool					Get(ReadBuffer& key, ReadBuffer& value);
	bool					Set(ReadBuffer& key, ReadBuffer& value, bool copy = true);
	void					Delete(ReadBuffer& key);

	StorageTable*			prev;
	StorageTable*			next;
	
private:
	void					WritePath(Buffer& buffer, uint32_t index);
	void					ReadTOC(uint32_t length);
	void					PerformRecovery(uint32_t length);
	void					WriteBackPages(InList<Buffer>& pages);
	void					DeleteGarbageFiles();
	void					RebuildTOC();
	void					WriteRecoveryPrefix();
	void					WriteRecoveryPostfix();
	void					WriteTOC();
	void					WriteData();	
	FileIndex*				Locate(ReadBuffer& key);
	void					SplitFile(StorageFile* file);

	StorageDataPage*		CursorBegin(ReadBuffer& key, Buffer& nextKey);

	void					CommitPhase1();
	void					CommitPhase2();
	void					CommitPhase3();
	
	int						tocFD;
	int						recoveryFD;
	uint32_t				prevCommitFileIndex;
	uint32_t				nextFileIndex;
	Buffer					name;
	Buffer					tocFilepath;
	Buffer					recoveryFilepath;
	Buffer					buffer;
	InTreeMap<FileIndex>	files;
	
	friend class StorageCursor;
	friend class StorageDatabase;
};

#endif
