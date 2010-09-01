#ifndef STORAGEDATAPAGE_H
#define STORAGEDATAPAGE_H

#include "System/Common.h"
#include "System/Buffers/Buffer.h"
#include "System/Buffers/ReadBuffer.h"
#include "System/Containers/InList.h"
#include "System/Containers/InTreeMap.h"
#include "StoragePage.h"

class StorageCursor; // forward

#define DATAPAGE_FIX_OVERHEAD		16
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

class StorageDataPage : public StoragePage
{
public:
	StorageDataPage(bool detached = false);
	~StorageDataPage();
	
	// basic ops	
	bool					Get(ReadBuffer& key, ReadBuffer& value);
	void					Set(ReadBuffer& key, ReadBuffer& value, bool copy = true);
	void					Delete(ReadBuffer& key);

	void					RegisterCursor(StorageCursor* cursor);
	void					UnregisterCursor(StorageCursor* cursor);
	KeyValue*				BeginIteration(ReadBuffer& key);
	KeyValue*				Next(KeyValue* it);
	
	bool					IsEmpty();
	ReadBuffer				FirstKey();
	bool					IsOverflowing();
	StorageDataPage*		SplitDataPage();
	bool					HasCursors();
	void					Detach();

	void					Read(ReadBuffer& buffer);
	void					Write(Buffer& buffer);

private:
	uint32_t				required;
	InTreeMap<KeyValue>		keys;
	InList<StorageCursor>	cursors;
	bool					detached;
};


#endif
