#include "IndexPage.h"
#include <stdio.h>

static int KeyCmp(const ReadBuffer& a, const ReadBuffer& b)
{
	return ReadBuffer::Cmp(a, b);
}

static ReadBuffer& Key(KeyIndex* kv)
{
	return kv->key;
}

bool KeyIndex::LessThan(KeyIndex& a, KeyIndex& b)
{
	return ReadBuffer::LessThan(a.key, b.key);
}

IndexPage::IndexPage()
{
	required = INDEXPAGE_FIX_OVERHEAD;
}

IndexPage::~IndexPage()
{
	keys.DeleteTree();
}

void IndexPage::SetPageSize(uint32_t pageSize_)
{
	pageSize = pageSize_;
}

void IndexPage::SetNumDataPageSlots(uint32_t numDataPageSlots_)
{
	uint32_t i;
	
	numDataPageSlots = numDataPageSlots_;

	freeDataPages.Clear();
		
	for (i = 0; i < numDataPageSlots; i++)
		freeDataPages.Add(i);
}

void IndexPage::Add(ReadBuffer key, uint32_t index, bool copy)
{
	KeyIndex* ki;
	
	required += INDEXPAGE_KV_OVERHEAD + key.GetLength();

	ki = new KeyIndex;
	ki->SetKey(key, copy);
	ki->index = index;
	
	keys.Insert(ki);
	
	freeDataPages.Remove(index);
}

void IndexPage::Update(ReadBuffer key, uint32_t index, bool copy)
{
	KeyIndex* it;
	
	for (it = keys.First(); it != NULL; it = keys.Next(it))
	{
		if (it->index == index)
		{
			required -= it->key.GetLength();
			it->SetKey(key, copy);
			required -= it->key.GetLength();
			return;
		}
	}
}

void IndexPage::Remove(ReadBuffer key)
{
	KeyIndex*	it;
	
	it = keys.Remove<ReadBuffer&>(key);
	if (!it)
		ASSERT_FAIL();
	
	required -= (INDEXPAGE_KV_OVERHEAD + it->key.GetLength());
	freeDataPages.Add(it->index);
	delete it;
}

bool IndexPage::IsEmpty()
{
	if (keys.GetCount() == 0)
		return true;
	else
		return false;
}

ReadBuffer IndexPage::FirstKey()
{
	if (keys.GetCount() == 0)
		ASSERT_FAIL();
	
	return keys.First()->key;
}

uint32_t IndexPage::NumEntries()
{
	return keys.GetCount();
}

int32_t IndexPage::Locate(ReadBuffer& key, Buffer* nextKey)
{
	KeyIndex*	it;
	uint32_t	index;
	int			cmpres;
	
	if (keys.GetCount() == 0)
		return -1;
		
	if (ReadBuffer::LessThan(key, keys.First()->key))
	{
		index = keys.First()->index;
		it = keys.Next(keys.First());
		if (it)
			nextKey->Write(it->key);
		return index;
	}
	
	it = keys.Locate<ReadBuffer&>(key, cmpres);
	if (cmpres >= 0)
		index = it->index;
	else
		index = keys.Prev(it)->index;

	it = keys.Next(it);
	if (it)
		nextKey->Write(it->key);

	return index;
}

uint32_t IndexPage::NextFreeDataPage()
{
	assert(freeDataPages.GetLength() > 0);
	
	return *freeDataPages.First();
}

bool IndexPage::IsOverflowing()
{
	if (required < pageSize)
		return false;
	else
		return true;
}

void IndexPage::Read(ReadBuffer& buffer_)
{
	uint32_t	num, len, i;
	char*		p;
	KeyIndex*	ki;

	buffer.Write(buffer_);

	p = buffer.GetBuffer();
	num = FromLittle32(*((uint32_t*) p));
	p += 4;
	for (i = 0; i < num; i++)
	{
		ki = new KeyIndex;
		len = FromLittle32(*((uint32_t*) p));
		p += 4;
		ki->key.SetLength(len);
		ki->key.SetBuffer(p);
		p += len;
		ki->index = FromLittle32(*((uint32_t*) p));
//		printf("%.*s => %u\n", ki->key.GetLength(), ki->key.GetBuffer(), ki->index);
		p += 4;
		keys.Insert(ki);
		freeDataPages.Remove(ki->index);
	}
	
	required = p - buffer.GetBuffer();
}

void IndexPage::Write(Buffer& buffer)
{
	KeyIndex*	it;
	char*		p;
	unsigned	len;

	p = buffer.GetBuffer();
	*((uint32_t*) p) = ToLittle32(keys.GetCount());
	p += 4;
	for (it = keys.First(); it != NULL; it = keys.Next(it))
	{
//		printf("writing index: %.*s => %u\n", it->key.GetLength(), it->key.GetBuffer(), it->index);
		len = it->key.GetLength();
		*((uint32_t*) p) = ToLittle32(len);
		p += 4;
		memcpy(p, it->key.GetBuffer(), len);
		p += len;
		*((uint32_t*) p) = ToLittle32(it->index);
		p += 4;
	}
	
	buffer.SetLength(required);
}

KeyIndex::KeyIndex()
{
	keyBuffer = NULL;
}

KeyIndex::~KeyIndex()
{
	if (keyBuffer != NULL)
		delete keyBuffer;
}

void KeyIndex::SetKey(ReadBuffer& key_, bool copy)
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

