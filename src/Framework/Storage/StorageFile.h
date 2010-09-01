#ifndef STORAGEFILE_H
#define STORAGEFILE_H

#include "StorageDefaults.h"
#include "StorageIndexPage.h"
#include "StorageDataPage.h"

#define INDEXPAGE_OFFSET			12

/*
===============================================================================

 StorageFile

===============================================================================
*/

class StorageFile
{
public:
	StorageFile();
	~StorageFile();
	
	void					Open(char* filepath);
	void					Close();
	
	void					SetStorageFileIndex(uint32_t fileIndex);

//	bool					IsNew();
	
	bool					Get(ReadBuffer& key, ReadBuffer& value);
	bool					Set(ReadBuffer& key, ReadBuffer& value, bool copy = true);
	void					Delete(ReadBuffer& key);

	ReadBuffer				FirstKey();
	bool					IsEmpty();

	bool					IsOverflowing();
	StorageFile*			SplitFile();

	void					Read();
	void					ReadRest();
	void					WriteRecovery(int recoveryFD);
	void					WriteData();

	StorageDataPage*		CursorBegin(ReadBuffer& key, Buffer& nextKey);

private:
	int32_t					Locate(ReadBuffer& key);
	void					LoadDataPage(uint32_t index);
	void					MarkPageDirty(StoragePage* page);
	void					SplitDataPage(uint32_t index);
	void					ReorderPages();
	void					ReorderFile();
	void					CopyDataPage(uint32_t index);
	
	int						fd;
	uint32_t				fileIndex;
	uint32_t				indexPageSize;
	uint32_t				dataPageSize;
	uint32_t				numDataPageSlots;
	bool					isOverflowing;
	bool					newFile;
	Buffer					filepath;
	StorageIndexPage		indexPage;
	StorageDataPage**		dataPages;
	uint32_t				numDataPages;
	InTreeMap<StoragePage>	dirtyPages;
};

#endif
