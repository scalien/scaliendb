#ifndef STORAGEFILEINDEX_H
#define STORAGEFILEINDEX_H

#include "System/Buffers/Buffer.h"
#include "System/Buffers/ReadBuffer.h"
#include "System/Containers/InTreeMap.h"
#include "StorageFile.h"

/*
===============================================================================

 StorageFileIndex

===============================================================================
*/

class StorageFileIndex
{
	typedef InTreeNode<StorageFileIndex> FileIndexNode;

public:
	StorageFileIndex();
	~StorageFileIndex();
	
	void					SetKey(ReadBuffer key, bool copy);

	Buffer					filepath;
	StorageFile*			file;

	ReadBuffer				key;
	Buffer*					keyBuffer;
	uint32_t				index;
	
	FileIndexNode			treeNode;
};

inline bool LessThan(StorageFileIndex &a, StorageFileIndex &b)
{
	return ReadBuffer::LessThan(a.key, b.key);
}

#endif
