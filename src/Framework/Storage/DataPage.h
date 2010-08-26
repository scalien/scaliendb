#ifndef DATAPAGE_H
#define DATAPAGE_H

#include "System/Common.h"
#include "System/Buffers/Buffer.h"
#include "System/Buffers/ReadBuffer.h"
#include "System/Containers/InList.h"
#include "System/Containers/InTreeMap.h"
#include "System/Stopwatch.h" // TODO REMOVE ME
#include "Page.h"

class KeyValue; // forward

#define DATAPAGE_FIX_OVERHEAD		4
#define DATAPAGE_KV_OVERHEAD		8
#define DATAPAGE_MAX_KV_SIZE(s)		((s) - (DATAPAGE_FIX_OVERHEAD + DATAPAGE_KV_OVERHEAD))

/*
===============================================================================

 KeyValue

===============================================================================
*/

class KeyValue
{
public:
	KeyValue();
	~KeyValue();

	void					SetKey(ReadBuffer& key, bool copy);
	void					SetValue(ReadBuffer& value, bool copy);

	ReadBuffer				key;
	ReadBuffer				value;
	
	Buffer*					keyBuffer;
	Buffer*					valueBuffer;
	
	KeyValue*				prev;
	KeyValue*				next;

	InTreeNode<KeyValue>	treeNode;
};

inline bool LessThan(KeyValue &a, KeyValue &b)
{
	return ReadBuffer::LessThan(a.key, b.key);
}

/*
===============================================================================

 DataPage

===============================================================================
*/

class DataPage : public Page
{
public:
	DataPage();
	~DataPage();
		
	bool					Get(ReadBuffer& key, ReadBuffer& value);
	void					Set(ReadBuffer& key, ReadBuffer& value, bool copy = true);
	void					Delete(ReadBuffer& key);

	bool					IsEmpty();
	ReadBuffer				FirstKey();
	bool					IsOverflowing();
	DataPage*				SplitDataPage();

	void					Read(ReadBuffer& buffer);
	void					Write(Buffer& buffer);

	static Stopwatch		sw1;
	
private:
	Buffer					buffer;
	InList<KeyValue>		kvs;
	uint32_t				required;
	InTreeMap<KeyValue>		keys;
};


#endif
