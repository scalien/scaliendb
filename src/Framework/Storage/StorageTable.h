#ifndef STORAGETABLE_H
#define STORAGETABLE_H

#include "System/Containers/InTreeMap.h"
#include "StorageFile.h"
#include "StorageCursor.h"
#include "StorageFileIndex.h"

class StorageDatabase; // forward

/*
===============================================================================

 StorageTable

===============================================================================
*/

class StorageTable
{
	typedef InTreeMap<StorageFileIndex> FileIndexMap;

public:
	~StorageTable();
	
	const char*				GetName();

	void					Open(const char* name);
	void					Commit(bool flush = true);
	void					Close();
		
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
	StorageFileIndex*		Locate(ReadBuffer& key);
	void					SplitFile(StorageFile* file);

	StorageDataPage*		CursorBegin(ReadBuffer& key, Buffer& nextKey);

	void					CommitPhase1();
	void					CommitPhase2();
	void					CommitPhase3();
	
	int						tocFD;
	int						recoveryFD;
	uint32_t				prevCommitStorageFileIndex;
	uint32_t				nextStorageFileIndex;
	Buffer					name;
	Buffer					tocFilepath;
	Buffer					recoveryFilepath;
	Buffer					buffer;
	FileIndexMap			files;
	
	friend class StorageCursor;
	friend class StorageDatabase;
};

#endif
