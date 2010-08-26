#ifndef CATALOG_H
#define CATALOG_H

#include "System/Containers/InTreeMap.h"
#include "File.h"

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
	File*					file;

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

 Catalog

===============================================================================
*/

class Catalog
{
public:
	~Catalog();
	
	void					Open(char* filepath);
	void					Flush();
	void					Close();
	
	bool					Get(ReadBuffer& key, ReadBuffer& value);
	bool					Set(ReadBuffer& key, ReadBuffer& value, bool copy = true);
	void					Delete(ReadBuffer& key);

private:
	void					WritePath(Buffer& buffer, uint32_t index);
	void					Read(uint32_t length);
	void					Write();
	FileIndex*				Locate(ReadBuffer& key);
	void					SplitFile(File* file);
	
	int						fd;
	uint32_t				nextFileIndex;
	Buffer					filepath;
	Buffer					buffer;
	InTreeMap<FileIndex>	files;
};

#endif
