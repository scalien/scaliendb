#include "StorageDataPage.h"
#include "StorageCursor.h"
#include <stdio.h>

static int KeyCmp(const ReadBuffer& a, const ReadBuffer& b)
{
	return ReadBuffer::Cmp(a, b);
}

static ReadBuffer& Key(KeyValue* kv)
{
	return kv->key;
}

StorageDataPage::StorageDataPage(bool detached_)
{
	required = DATAPAGE_FIX_OVERHEAD;
	detached = detached_;
}

StorageDataPage::~StorageDataPage()
{
	keys.DeleteTree();
}

bool StorageDataPage::Get(ReadBuffer& key, ReadBuffer& value)
{
	KeyValue*	kv;
	
	kv = keys.Get<ReadBuffer&>(key);
	if (kv != NULL)
	{
		value = kv->value;
		return true;
	}	
	
	return false;
}

void StorageDataPage::Set(ReadBuffer& key, ReadBuffer& value, bool copy)
{
	KeyValue*	it;
	KeyValue*	newKeyValue;

	int res;
	it = keys.Locate<ReadBuffer&>(key, res);
	if (res == 0 && it != NULL)
	{
		// found
		required -= it->value.GetLength();
		it->SetValue(value, copy);
		required += it->value.GetLength();
		return;
	}

	// not found
	newKeyValue = new KeyValue;
	newKeyValue->SetKey(key, copy);
	newKeyValue->SetValue(value, copy);
	keys.InsertAt(newKeyValue, it, res);
	required += (DATAPAGE_KV_OVERHEAD + key.GetLength() + value.GetLength());
}

void StorageDataPage::Delete(ReadBuffer& key)
{
	KeyValue*	it;

	it = keys.Remove<ReadBuffer&>(key);
	if (it)
	{
		required -= (DATAPAGE_KV_OVERHEAD + it->key.GetLength() + it->value.GetLength());
		delete it;
	}
}

void StorageDataPage::RegisterCursor(StorageCursor* cursor)
{
	cursors.Append(cursor);
}

void StorageDataPage::UnregisterCursor(StorageCursor* cursor)
{
	cursors.Remove(cursor);
	
	if (detached && cursors.GetLength() == 0)
		delete this;
}

KeyValue* StorageDataPage::BeginIteration(ReadBuffer& key)
{
	KeyValue*	it;
	int			retcmp;
	
	it = keys.Locate(key, retcmp);
	
	if (it == NULL)
		return NULL;
	
	if (retcmp <= 0)
		return it;
	else
		return keys.Next(it);
}

KeyValue* StorageDataPage::Next(KeyValue* it)
{
	return keys.Next(it);
}

bool StorageDataPage::IsEmpty()
{
	if (keys.GetCount() == 0)
		return true;
	else
		return false;
}

ReadBuffer StorageDataPage::FirstKey()
{
	return keys.First()->key;
}

bool StorageDataPage::IsOverflowing()
{
	if (required <= pageSize)
		return false;
	else
		return true;
}

StorageDataPage* StorageDataPage::SplitDataPage()
{
	StorageDataPage*	newPage;
	KeyValue*			it;
	KeyValue*			next;
	uint32_t			target, sum;
	
	if (required > 2 * pageSize)
		ASSERT_FAIL();
		
	target = required / 2;
	sum = DATAPAGE_FIX_OVERHEAD;
	for (it = keys.First(); it != NULL; it = keys.Next(it))
	{
		sum += (it->key.GetLength() + it->value.GetLength() + DATAPAGE_KV_OVERHEAD);
		if (sum > pageSize || sum >= target)
			break;		
	}
	
	assert(it != NULL);
	
	newPage = new StorageDataPage();
	newPage->SetPageSize(pageSize);
	newPage->SetFileIndex(fileIndex);
	
	while (it != NULL)
	{
		required -= (DATAPAGE_KV_OVERHEAD + it->key.GetLength() + it->value.GetLength());
		// TODO: optimize buffer to avoid delete and realloc below
		next = keys.Next(it);
		keys.Remove(it);
		newPage->keys.Insert(it);
		newPage->required += (DATAPAGE_KV_OVERHEAD + it->key.GetLength() + it->value.GetLength());
		it = next;
	}
	
	assert(IsEmpty() != true);
	assert(newPage->IsEmpty() != true);
	return newPage;
}

bool StorageDataPage::HasCursors()
{
	return (cursors.GetLength() > 0);
}

void StorageDataPage::Detach()
{
	StorageDataPage*	detached;
	StorageCursor*		cursor;
	KeyValue*			it;
	KeyValue*			kv;
	int					cmpres;
	
	detached = new StorageDataPage(true);
	
	detached->cursors = cursors;
	cursors.ClearMembers();
	
	for (it = keys.First(); it != NULL; it = keys.Next(it))
	{
		kv = new KeyValue;
		kv->SetKey(it->key, true);
		kv->SetValue(it->value, true);
		detached->keys.Insert(kv);
	}
	
	for (cursor = detached->cursors.First(); cursor != NULL; cursor = detached->cursors.Next(cursor))
	{
		cursor->dataPage = detached;
		if (cursor->current != NULL)
		{
			cursor->current = detached->keys.Locate(cursor->current->key, cmpres);
			assert(cmpres == 0);
		}
	}
}

void StorageDataPage::Read(ReadBuffer& buffer_)
{
	uint32_t	num, len, i;
	char*		p;
	KeyValue*	kv;

	buffer.Write(buffer_);
	
	p = buffer.GetBuffer();
	pageSize = FromLittle32(*((uint32_t*) p));
	p += 4;
	fileIndex = FromLittle32(*((uint32_t*) p));
	p += 4;
	offset = FromLittle32(*((uint32_t*) p));
	p += 4;
	num = FromLittle32(*((uint32_t*) p));
	p += 4;
	for (i = 0; i < num; i++)
	{
		kv = new KeyValue;
		len = FromLittle32(*((uint32_t*) p));
		p += 4;
		kv->key.SetLength(len);
		kv->key.SetBuffer(p);
		// TODO: hack
		kv->keyBuffer = new Buffer();
		kv->keyBuffer->Write(kv->key);
		kv->key.Wrap(*kv->keyBuffer);
		p += len;
		len = FromLittle32(*((uint32_t*) p));
		p += 4;
		kv->value.SetLength(len);
		kv->value.SetBuffer(p);
		// TODO: hack
		kv->valueBuffer = new Buffer();
		kv->valueBuffer->Write(kv->value);
		kv->value.Wrap(*kv->valueBuffer);
		
		p += len;
		printf("read %.*s => %.*s\n", P(&(kv->key)), P(&(kv->value)));
		keys.Insert(kv);
	}
	
	required = p - buffer.GetBuffer();
	assert(IsEmpty() != true);
}

void StorageDataPage::Write(Buffer& buffer)
{
	KeyValue*	it;
	char*		p;
	unsigned	len;
	uint32_t	num;

	p = buffer.GetBuffer();
	*((uint32_t*) p) = ToLittle32(pageSize);
	p += 4;
	assert(fileIndex != 0);
	*((uint32_t*) p) = ToLittle32(fileIndex);
	p += 4;
	*((uint32_t*) p) = ToLittle32(offset);
	p += 4;
	num = keys.GetCount();
	*((uint32_t*) p) = ToLittle32(num);
	p += 4;
	for (it = keys.First(); it != NULL; it = keys.Next(it))
	{
		len = it->key.GetLength();
		*((uint32_t*) p) = ToLittle32(len);
		p += 4;
		memcpy(p, it->key.GetBuffer(), len);
		p += len;
		len = it->value.GetLength();
		*((uint32_t*) p) = ToLittle32(len);
		p += 4;
		memcpy(p, it->value.GetBuffer(), len);
		p += len;
		printf("writing %.*s => %.*s\n", P(&(it->key)), P(&(it->value)));
	}
	
	buffer.SetLength(required);
	this->buffer.Write(buffer);
}

KeyValue::KeyValue()
{
	keyBuffer = NULL;
	valueBuffer = NULL;
}

KeyValue::~KeyValue()
{
	if (keyBuffer != NULL)
		delete keyBuffer;
	if (valueBuffer != NULL)
		delete valueBuffer;
}

void KeyValue::SetKey(ReadBuffer& key_, bool copy)
{
	if (keyBuffer != NULL && !copy)
	{
		delete keyBuffer;
		keyBuffer = NULL;
	}
	
	if (copy)
	{
		if (keyBuffer == NULL)
			keyBuffer = new Buffer; // TODO: allocation strategy
		keyBuffer->Write(key_);
		key.Wrap(*keyBuffer);
	}
	else
		key = key_;
}

void KeyValue::SetValue(ReadBuffer& value_, bool copy)
{
	if (valueBuffer != NULL && !copy)
	{
		delete valueBuffer;
		valueBuffer = NULL;
	}

	if (copy)
	{
		if (valueBuffer == NULL)
			valueBuffer = new Buffer; // TODO: allocation strategy
		valueBuffer->Write(value_);
		value.Wrap(*valueBuffer);
	}
	else
		value = value_;
}

