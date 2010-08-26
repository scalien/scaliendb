#include "DataPage.h"
#include <stdio.h>

static int KeyCmp(const ReadBuffer& a, const ReadBuffer& b)
{
	return ReadBuffer::Cmp(a, b);
}

static ReadBuffer Key(KeyValue* kv)
{
	return kv->key;
}

DataPage::DataPage()
{
	required = DATAPAGE_FIX_OVERHEAD;
}

DataPage::~DataPage()
{
}

bool DataPage::Get(ReadBuffer& key, ReadBuffer& value)
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

Stopwatch DataPage::sw1;

void DataPage::Set(ReadBuffer& key, ReadBuffer& value, bool copy)
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

void DataPage::Delete(ReadBuffer& key)
{
	KeyValue*	it;

	it = keys.Remove<ReadBuffer&>(key);
	if (it)
		required -= (DATAPAGE_KV_OVERHEAD + it->key.GetLength() + it->value.GetLength());
}

bool DataPage::IsEmpty()
{
	if (keys.GetCount() == 0)
		return true;
	else
		return false;
}

ReadBuffer DataPage::FirstKey()
{
	return keys.First()->key;
}

bool DataPage::IsOverflowing()
{
	if (required <= pageSize)
		return false;
	else
		return true;
}

DataPage* DataPage::SplitDataPage()
{
	DataPage*	newPage;
	KeyValue*	it;
	KeyValue*	next;
	uint32_t	target, sum;
	
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
	
	newPage = new DataPage();
	newPage->SetPageSize(pageSize);
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
	
	return newPage;
}

void DataPage::Read(ReadBuffer& buffer_)
{
	uint32_t	num, len, i;
	char*		p;
	KeyValue*	kv;

	buffer.Write(buffer_);
	
	p = buffer.GetBuffer();
	num = FromLittle32(*((uint32_t*) p));
//	printf("has %u keys\n", num);
	p += 4;
	for (i = 0; i < num; i++)
	{
		kv = new KeyValue;
		len = FromLittle32(*((uint32_t*) p));
		p += 4;
		kv->key.SetLength(len);
		kv->key.SetBuffer(p);
		p += len;
		len = FromLittle32(*((uint32_t*) p));
		p += 4;
		kv->value.SetLength(len);
		kv->value.SetBuffer(p);
		p += len;
//		printf("read %.*s => %.*s\n", P(&(kv->key)), P(&(kv->value)));
		keys.Insert(kv);
	}
	
	required = p - buffer.GetBuffer();
}

void DataPage::Write(Buffer& buffer)
{
	KeyValue*	it;
	char*		p;
	unsigned	len;
	uint32_t	num;

	p = buffer.GetBuffer();
	num = keys.GetCount();
	*((uint32_t*) p) = ToLittle32(num);
//	printf("has %u keys\n", num);
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
//		printf("writing %.*s => %.*s\n", P(&(it->key)), P(&(it->value)));
	}
	
	buffer.SetLength(required);
}

KeyValue::KeyValue()
{
	keyBuffer = NULL;
	valueBuffer = NULL;
	prev = this;
	next = this;
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
		delete keyBuffer;
	
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
		delete valueBuffer;

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

