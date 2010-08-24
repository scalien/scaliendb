#include "DataPage.h"

static int KeyCmp(ReadBuffer a, ReadBuffer b)
{
	// TODO: this is ineffective
	if (ReadBuffer::LessThan(a, b))
		return -1;
	if (ReadBuffer::LessThan(b, a))
		return 1;
	return 0;
}

static ReadBuffer Key(KeyValue* kv)
{
	return kv->key;
}

DataPage::DataPage()
{
	required = DATAPAGE_FIX_OVERHEAD;
}

void DataPage::SetPageSize(uint32_t pageSize_)
{
	pageSize = pageSize_;
}

bool DataPage::Get(ReadBuffer& key, ReadBuffer& value)
{
	KeyValue* it;
	
	for (it = kvs.Head(); it != NULL; it = kvs.Next(it))
	{
		if (ReadBuffer::LessThan(key, it->key))
			break;

		if (BUFCMP(&it->key, &key))
		{
			value = it->value;
			return true;
		}
	}
	
//	it = kvs_.Get<ReadBuffer&>(key);
//	if (it != NULL)
//	{
//		value = it->value;
//		return true;
//	}	
	
	return false;
}

Stopwatch DataPage::sw1;

void DataPage::Set(ReadBuffer& key, ReadBuffer& value, bool copy)
{
	KeyValue*	it;
	KeyValue*	newKeyValue;

	sw1.Start();
	for (it = kvs.Head(); it != NULL; it = kvs.Next(it))
	{
		if (ReadBuffer::LessThan(key, it->key))
			break;

		if (BUFCMP(&it->key, &key))
		{
			// found
			required -= it->value.GetLength();
			it->SetValue(value, copy);
			required += it->value.GetLength();
			return;
		}
	}
	sw1.Stop();
	
	if (it != NULL)
		it = kvs.Prev(it);
	else
		it = kvs.Tail();
	
	// not found
	newKeyValue = new KeyValue;
	newKeyValue->SetKey(key, copy);
	newKeyValue->SetValue(value, copy);
	kvs.InsertAfter(it, newKeyValue);
	required += (DATAPAGE_KV_OVERHEAD + key.GetLength() + value.GetLength());

//	kvs_.Insert(newKeyValue);
}

void DataPage::Delete(ReadBuffer& key)
{
	KeyValue*	it;

	// delete from list
	for (it = kvs.Head(); it != NULL; it = kvs.Next(it))
	{
		if (ReadBuffer::LessThan(key, it->key))
			break;

		if (BUFCMP(&it->key, &key))
		{
			// found
			required -= (DATAPAGE_KV_OVERHEAD + it->key.GetLength() + it->value.GetLength());
			kvs.Delete(it);
			break;
		}
	}
	
	// TODO: Delete is not implemented!
//	kvs_.Delete<ReadBuffer&>(key);
}

bool DataPage::IsEmpty()
{
	if (kvs.GetLength() == 0)
		return true;
	else
		return false;
}

ReadBuffer DataPage::FirstKey()
{
	return kvs.Head()->key;
}

bool DataPage::MustSplit()
{
	if (required <= pageSize)
		return false;
	else
		return true;
}

DataPage* DataPage::Split()
{
	DataPage*	newPage;
	KeyValue*	it;
	KeyValue*	t;
	uint32_t	target, sum;
	
	if (required > 2 * pageSize)
		ASSERT_FAIL();
		
	target = required / 2;
	sum = DATAPAGE_FIX_OVERHEAD;
	for (it = kvs.Head(); it != NULL; it = kvs.Next(it))
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
		t = kvs.Remove(it);
		newPage->kvs.Append(it);
		newPage->required += (DATAPAGE_KV_OVERHEAD + it->key.GetLength() + it->value.GetLength());
		it = t;
	}
	
	return newPage;
}

void DataPage::Read(ReadBuffer& buffer)
{
	uint32_t	num, len, i;
	char*		p;
	KeyValue*	kv;

	required = buffer.GetLength();

	p = buffer.GetBuffer();
	num = FromLittle32(*((uint32_t*) p));
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
		
		kvs.Append(kv);
	}
}

void DataPage::Write(Buffer& buffer)
{
	KeyValue*	it;
	char*		p;
	unsigned	len;

	buffer.Allocate(required);

	p = buffer.GetBuffer();
	*((uint32_t*) p) = ToLittle32(kvs.GetLength());
	p += 4;
	for (it = kvs.Head(); it != NULL; it = kvs.Next(it))
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
	}
	
	buffer.SetLength(required);
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

