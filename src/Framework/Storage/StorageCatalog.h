#ifndef STORAGECATALOG_H
#define STORAGECATALOG_H

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
	
	void					SetKey(ReadBuffer& key, bool copy);

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

/*
===============================================================================

 StorageCatalog

===============================================================================
*/

class StorageCatalog
{
public:
	~StorageCatalog();
	
	void					Open(const char* filepath);
	void					Flush();
	void					Close();
	
	bool					Get(ReadBuffer& key, ReadBuffer& value);
	bool					Set(ReadBuffer& key, ReadBuffer& value, bool copy = true);
	void					Delete(ReadBuffer& key);

private:
	void					WritePath(Buffer& buffer, uint32_t index);
	void					Read(uint32_t length);
	void					Write(bool flush);
	FileIndex*				Locate(ReadBuffer& key);
	void					SplitFile(StorageFile* file);

	StorageDataPage*		CursorBegin(ReadBuffer& key, Buffer& nextKey);
	
	int						fd;
	uint32_t				nextFileIndex;
	Buffer					filepath;
	Buffer					buffer;
	InTreeMap<FileIndex>	files;
	
	friend class StorageCursor;
};

#endif
