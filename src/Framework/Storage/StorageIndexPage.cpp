#include "StorageIndexPage.h"
#include "StorageDefaults.h"

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

// TODO: this should be renamed because it also updates the KeyIndex
void StorageIndexPage::Add(ReadBuffer key, uint32_t index, bool copy)
{
    StorageKeyIndex*    ki;
    bool                ret;
    
    //Log_Message("key = %.*s, index = %u", P(&key), (unsigned) index);
    
    ST_ASSERT(keys.Get(key) == NULL);
    
    ki = keys.Get(key);
    required += INDEXPAGE_KV_OVERHEAD + key.GetLength();

    ki = new StorageKeyIndex;
    ki->SetKey(key, copy);
    ki->index = index;
    
    keys.Insert(ki);
    ST_ASSERT(keys.GetCount() <= DEFAULT_NUM_DATAPAGES);
    
    ret = freeDataPages.Remove(index);
    ST_ASSERT(ret == true);
    
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
            keys.Remove(it);
            it->SetKey(key, copy);
            keys.Insert(it);
            required += it->key.GetLength();
            return;
        }
    }
}

void StorageIndexPage::Remove(ReadBuffer key)
{
    StorageKeyIndex*    it;
    uint32_t            index;

    it = keys.Get<ReadBuffer&>(key);
    ST_ASSERT(it != NULL);
    keys.Remove(it);
    ST_ASSERT(ReadBuffer::Cmp(key, it->key) == 0);
    
    required -= (INDEXPAGE_KV_OVERHEAD + it->key.GetLength());
    freeDataPages.Add(it->index);
    index = it->index;
    delete it;

    UpdateHighestIndex(index);
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
    ST_ASSERT(keys.GetCount() != 0);
    
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
    uint32_t            index;
    int                 cmpres;
    
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
    ST_ASSERT(freeDataPages.GetLength() > 0);
    
    return *freeDataPages.First();
}

bool StorageIndexPage::IsOverflowing()
{
    if (required < pageSize && keys.GetCount() < DEFAULT_NUM_DATAPAGES)
        return false;
    else
        return true;
}

void StorageIndexPage::Read(ReadBuffer& buffer_)
{
    uint32_t            num, len, i;
    StorageKeyIndex*    ki;
    StorageKeyIndex*    oldKi;
    ReadBuffer          tmp;
    unsigned            numEmpty;

    buffer.Write(buffer_);
    tmp = buffer;
    
    maxDataPageIndex = -1;

    tmp.ReadLittle32(pageSize);
    tmp.Advance(sizeof(uint32_t));
    tmp.ReadLittle32(fileIndex);
    tmp.Advance(sizeof(uint32_t));
    tmp.ReadLittle32(offset);
    tmp.Advance(sizeof(uint32_t));
    tmp.ReadLittle32(num);
    tmp.Advance(sizeof(uint32_t));

    STORAGE_TRACE("reading index for file %u\n", fileIndex);
    numEmpty = 0;
    for (i = 0; i < num; i++)
    {
        ki = new StorageKeyIndex;

        tmp.ReadLittle32(len);
        tmp.Advance(sizeof(uint32_t));

        if (len == 0)
            numEmpty++;

        ki->key.SetLength(len);
        ki->key.SetBuffer(tmp.GetBuffer());
        tmp.Advance(len);

        tmp.ReadLittle32(ki->index);
        tmp.Advance(sizeof(uint32_t));

        oldKi = keys.Insert(ki);
        ST_ASSERT(oldKi == NULL);
        freeDataPages.Remove(ki->index);
        if ((int32_t) ki->index > maxDataPageIndex)
            maxDataPageIndex = ki->index;
    }
    
    required = tmp.GetBuffer() - buffer.GetBuffer();
    
    ST_ASSERT(keys.GetCount() == numDataPageSlots - freeDataPages.GetLength());
    ST_ASSERT(numEmpty <= 1);
}

bool StorageIndexPage::CheckWrite(Buffer& buffer)
{
    StorageKeyIndex*    it;
    unsigned            len;

    this->buffer.Allocate(pageSize);

    buffer.SetLength(0);

    ST_ASSERT(fileIndex != 0);
    ST_ASSERT(keys.GetCount() == numDataPageSlots - freeDataPages.GetLength());

    buffer.AppendLittle32(pageSize);
    buffer.AppendLittle32(fileIndex);
    buffer.AppendLittle32(offset);
    buffer.AppendLittle32(keys.GetCount());
    STORAGE_TRACE("writing index for file %u\n", fileIndex);

    FOREACH (it, keys)
    {
        STORAGE_TRACE("writing index: %.*s => %u\n", it->key.GetLength(), it->key.GetBuffer(), it->index);
        len = it->key.GetLength();
        buffer.AppendLittle32(len);
        buffer.Append(it->key.GetBuffer(), len);
        buffer.AppendLittle32(it->index);
    }

    ST_ASSERT(required == buffer.GetLength());
    ST_ASSERT((unsigned) (maxDataPageIndex + 1) == numDataPageSlots - freeDataPages.GetLength());
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

void StorageIndexPage::UpdateHighestIndex(uint32_t index)
{
//  uint32_t            prevIndex;
//  uint32_t*           iit;
    StorageKeyIndex*    it;

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
