#include "IndexPage.h"
#include <stdio.h>

IndexPage::IndexPage()
{
	required = INDEXPAGE_FIX_OVERHEAD;
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
	
	keys.Add(ki);
	
	freeDataPages.Remove(index);
}

void IndexPage::Remove(uint32_t index)
{
	KeyIndex* it;
	
	for (it = keys.Head(); it != NULL; it = keys.Next(it))
	{
		if (it->index == index)
		{
			required -= (INDEXPAGE_KV_OVERHEAD + it->key.GetLength());
			keys.Delete(it);
			freeDataPages.Add(index);
			return;
		}
	}
}

bool IndexPage::IsEmpty()
{
	if (keys.GetLength() == 0)
		return true;
	else
		return false;
}

ReadBuffer IndexPage::FirstKey()
{
	if (keys.GetLength() == 0)
		ASSERT_FAIL();
	
	return keys.Head()->key;
}

uint32_t IndexPage::NumEntries()
{
	return keys.GetLength();
}

int32_t IndexPage::Locate(ReadBuffer& key)
{
	KeyIndex* it;
	
	if (keys.GetLength() == 0)
		return -1;
		
	if (LessThan(key, keys.Head()->key))
		return 0;
	
	for (it = keys.Head(); it != NULL; it = keys.Next(it))
	{
		if (LessThan(key, it->key))
			return keys.Prev(it)->index;
	}
	
	return keys.Tail()->index;
}

uint32_t IndexPage::NextFreeDataPage()
{
	assert(freeDataPages.GetLength() > 0);
	
	return *freeDataPages.Head();
}

bool IndexPage::IsOverflowing()
{
	return (required < pageSize);
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
		keys.Add(ki);
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
	*((uint32_t*) p) = ToLittle32(keys.GetLength());
	p += 4;
	for (it = keys.Head(); it != NULL; it = keys.Next(it))
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
	prev = this;
	next = this;
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

