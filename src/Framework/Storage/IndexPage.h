#ifndef INDEXPAGE_H
#define INDEXPAGE_H

#include "System/Platform.h"
#include "System/Common.h"
#include "System/Buffers/ReadBuffer.h"
#include "System/Containers/SortedList.h"
#include "System/Containers/InSortedList.h"
#include "System/Containers/InTreeMap.h"
#include "Page.h"

class KeyIndex; // forward
class File;		// forward

#define INDEXPAGE_FIX_OVERHEAD		4
#define INDEXPAGE_KV_OVERHEAD		8


inline bool LessThan(uint32_t a, uint32_t b)
{
	return (a < b);
}

/*
===============================================================================

 KeyIndex

===============================================================================
*/

class KeyIndex
{
public:
	KeyIndex();
	~KeyIndex();
	
	void					SetKey(ReadBuffer& key, bool copy);

	ReadBuffer				key;
	Buffer*					keyBuffer;
	uint32_t				index;

	InTreeNode<KeyIndex>	treeNode;

	static bool				LessThan(KeyIndex &a, KeyIndex &b);
};

/*
===============================================================================

 IndexPage

===============================================================================
*/

class IndexPage : public Page
{
public:
	IndexPage();
	~IndexPage();
	
	void					SetPageSize(uint32_t pageSize);
	void					SetNumDataPageSlots(uint32_t numDataPageSlots);
	
	void					Add(ReadBuffer key, uint32_t index, bool copy = true);
	void					Update(ReadBuffer key, uint32_t index, bool copy = true);
	void					Remove(ReadBuffer key);

	bool					IsEmpty();
	ReadBuffer				FirstKey();
	uint32_t				NumEntries();
	int32_t					Locate(ReadBuffer& key);
	uint32_t				NextFreeDataPage();
	bool					IsOverflowing();

	void					Read(ReadBuffer& buffer);
	void					Write(Buffer& buffer);
	
private:
	Buffer					buffer;
	uint32_t				numDataPageSlots;
	uint32_t				required;
	InTreeMap<KeyIndex>		keys;
	SortedList<uint32_t>	freeDataPages;
	
	friend class File;
};


#endif
