#include "StorageIndexPage.h"
#include <stdio.h>

static int KeyCmp(const ReadBuffer& a, const ReadBuffer& b)
{
    return ReadBuffer::Cmp(a, b);
}

static ReadBuffer& Key(StorageKeyIndex* kv)
{
    return kv->key;
}

bool StorageKeyIndex::LessThan(StorageKeyIndex& a, StorageKeyIndex& b)
{
    return ReadBuffer::LessThan(a.key, b.key);
}

StorageIndexPage::StorageIndexPage()
{
    required = INDEXPAGE_FIX_OVERHEAD;
    type = STORAGE_INDEX_PAGE;
    maxDataPageIndex = -1;
}

StorageIndexPage::~StorageIndexPage()
{
    keys.DeleteTree();
}

void StorageIndexPage::SetPageSize(uint32_t pageSize_)
{
    pageSize = pageSize_;
}

void StorageIndexPage::SetNumDataPageSlots(uint32_t numDataPageSlots_)
{
    uint32_t i;
    
    numDataPageSlots = numDataPageSlots_;

    freeDataPages.Clear();
        
    for (i = 0; i < numDataPageSlots; i++)
        freeDataPages.Add(i);
}

void StorageIndexPage::Add(ReadBuffer key, uint32_t index, bool copy)
{
    StorageKeyIndex*    ki;
    bool                ret;
    
    required += INDEXPAGE_KV_OVERHEAD + key.GetLength();

    ki = new StorageKeyIndex;
    ki->SetKey(key, copy);
    ki->index = index;
    
    keys.Insert(ki);
    
    ret = freeDataPages.Remove(index);
    assert(ret == true);
    
    if ((int32_t) index > maxDataPageIndex)
        maxDataPageIndex = index;
}

void StorageIndexPage::Update(ReadBuffer key, uint32_t index, bool copy)
{
    StorageKeyIndex* it;
    
    for (it = keys.First(); it != NULL; it = keys.Next(it))
    {
        if (it->index == index)
        {
            required -= it->key.GetLength();
            it->SetKey(key, copy);
            required += it->key.GetLength();
            return;
        }
    }
}

void StorageIndexPage::Remove(ReadBuffer key)
{
    StorageKeyIndex*    it;
    uint32_t            index;
//  uint32_t            prevIndex;
//  uint32_t*           iit;

    it = keys.Get<ReadBuffer&>(key);
    if (!it)
        ASSERT_FAIL();
    keys.Remove(it);
    
    required -= (INDEXPAGE_KV_OVERHEAD + it->key.GetLength());
    freeDataPages.Add(it->index);
    index = it->index;
    delete it;

    // maintain the new highest data page index
    if ((int32_t) index != maxDataPageIndex)
        return;

    maxDataPageIndex = -1;
    for (it = keys.First(); it != NULL; it = keys.Next(it))
    {
        if ((int32_t) it->index > maxDataPageIndex)
            maxDataPageIndex = it->index;
    }
        
    // TODO: debug this optimized version
//  // if the last free page is not the last page, then
//  // the last page is the highest page index 
//  iit = freeDataPages.Last();
//  if (*iit < numDataPageSlots - 1)
//  {
//      maxDataPageIndex = numDataPageSlots - 1;
//      return;
//  }
//  
//  // iterate through the free list and find a hole in it
//  prevIndex = *iit;
//  iit = freeDataPages.First();
//  maxDataPageIndex = *iit - 1;
//  for (iit = freeDataPages.Last(); iit != NULL; iit = freeDataPages.Prev(iit))
//  {
//      if (prevIndex - *iit > 1)
//      {
//          maxDataPageIndex = prevIndex - 1;
//          return;
//      }
//      prevIndex = *iit;
//  }
}

bool StorageIndexPage::IsEmpty()
{
    if (keys.GetCount() == 0)
        return true;
    else
        return false;
}

ReadBuffer StorageIndexPage::FirstKey()
{
    if (keys.GetCount() == 0)
        ASSERT_FAIL();
    
    return keys.First()->key;
}

uint32_t StorageIndexPage::NumEntries()
{
    return keys.GetCount();
}

int32_t StorageIndexPage::GetMaxDataPageIndex()
{
    return maxDataPageIndex;
}

int32_t StorageIndexPage::Locate(ReadBuffer& key, Buffer* nextKey)
{
    StorageKeyIndex*    it;
    uint32_t    index;
    int         cmpres;
    
    if (keys.GetCount() == 0)
        return -1;
        
    if (ReadBuffer::LessThan(key, keys.First()->key))
    {
        index = keys.First()->index;
        it = keys.Next(keys.First());
        if (it && nextKey)
            nextKey->Write(it->key);
        return index;
    }
    
    it = keys.Locate<ReadBuffer&>(key, cmpres);
    if (cmpres >= 0)
        index = it->index;
    else
        index = keys.Prev(it)->index;

    it = keys.Next(it);
    if (it && nextKey)
        nextKey->Write(it->key);

    return index;
}

uint32_t StorageIndexPage::NextFreeDataPage()
{
    assert(freeDataPages.GetLength() > 0);
    
    return *freeDataPages.First();
}

bool StorageIndexPage::IsOverflowing()
{
    if (required < pageSize)
        return false;
    else
        return true;
}

void StorageIndexPage::Read(ReadBuffer& buffer_)
{
    uint32_t            num, len, i;
    char*               p;
    StorageKeyIndex*    ki;

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
//  printf("reading index for file %u\n", fileIndex);
    for (i = 0; i < num; i++)
    {
        ki = new StorageKeyIndex;
        len = FromLittle32(*((uint32_t*) p));
        p += 4;
        ki->key.SetLength(len);
        ki->key.SetBuffer(p);
        p += len;
        ki->index = FromLittle32(*((uint32_t*) p));
//      printf("reading index %.*s => %u\n", ki->key.GetLength(), ki->key.GetBuffer(), ki->index);
        p += 4;
        keys.Insert(ki);
        freeDataPages.Remove(ki->index);
        if ((int32_t) ki->index > maxDataPageIndex)
            maxDataPageIndex = ki->index;
    }
    
    required = p - buffer.GetBuffer();
}

bool StorageIndexPage::CheckWrite(Buffer& buffer)
{
    StorageKeyIndex*    it;
    char*       p;
    unsigned    len;

    this->buffer.Allocate(pageSize);

    p = buffer.GetBuffer();
    *((uint32_t*) p) = ToLittle32(pageSize);
    p += 4;
    assert(fileIndex != 0);
    *((uint32_t*) p) = ToLittle32(fileIndex);
    p += 4;
    *((uint32_t*) p) = ToLittle32(offset);
    p += 4;
    assert(keys.GetCount() <= numDataPageSlots);
    *((uint32_t*) p) = ToLittle32(keys.GetCount());
    p += 4;
//  printf("writing index for file %u\n", fileIndex);
    for (it = keys.First(); it != NULL; it = keys.Next(it))
    {
//      printf("writing index: %.*s => %u\n", it->key.GetLength(), it->key.GetBuffer(), it->index);
        len = it->key.GetLength();
        *((uint32_t*) p) = ToLittle32(len);
        p += 4;
        memcpy(p, it->key.GetBuffer(), len);
        p += len;
        *((uint32_t*) p) = ToLittle32(it->index);
        p += 4;
    }
    assert(required == (p - buffer.GetBuffer()));
    buffer.SetLength(required);
    if (BUFCMP(&buffer, &this->buffer))
        return false;
    
    return true;
}

bool StorageIndexPage::Write(Buffer& buffer)
{
    bool ret;
    
    ret = CheckWrite(buffer);
    
    this->buffer.Write(buffer);
    
    return ret;
}
