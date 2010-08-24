#ifndef INDEXPAGE_H
#define INDEXPAGE_H

#include "Platform.h"

#include "Common.h"
#include "System/Buffers/ReadBuffer.h"
#include "System/Containers/SortedList.h"
#include "System/Containers/InSortedList.h"
#include "Page.h"

class KeyIndex; // forward

#define INDEXPAGE_FIX_OVERHEAD		4
#define INDEXPAGE_KV_OVERHEAD		8

/*
===============================================================================

 IndexPage

===============================================================================
*/

class IndexPage : public Page
{
public:
	IndexPage();
	
	void					SetPageSize(uint32_t pageSize);
	void					SetNumDataPageSlots(uint32_t numDataPageSlots);
	
	void					Add(ReadBuffer key, uint32_t index, bool copy = true);
	void					Remove(uint32_t index);

	bool					IsEmpty();
	ReadBuffer				FirstKey();
	int32_t					Locate(ReadBuffer& key);
	uint32_t				NextFreeDataPage();
	bool					MustSplit();

	void					Read(ReadBuffer& buffer);
	void					Write(Buffer& buffer);
	
private:
	uint32_t				pageSize;
	uint32_t				numDataPageSlots;
	uint32_t				required;
	InSortedList<KeyIndex>	keys;
	SortedList<uint32_t>	freeDataPages;
};


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
	
	KeyIndex*				prev;
	KeyIndex*				next;

	static bool				LessThan(KeyIndex &a, KeyIndex &b);
};

#endif
