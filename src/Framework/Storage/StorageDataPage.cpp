#include "StorageDataPage.h"
#include "StorageCursor.h"
#include "StorageDataCache.h"
#include "StorageDefaults.h"
#include <stdio.h>

static int KeyCmp(const ReadBuffer& a, const ReadBuffer& b)
{
    return ReadBuffer::Cmp(a, b);
}

static ReadBuffer& Key(StorageKeyValue* kv)
{
    return kv->key;
}

StorageDataPage::StorageDataPage(bool detached_)
{
    required = DATAPAGE_FIX_OVERHEAD;
    detached = detached_;
    type = STORAGE_DATA_PAGE;
    file = NULL;
}

StorageDataPage::~StorageDataPage()
{
    keys.DeleteTree();
}

bool StorageDataPage::Get(ReadBuffer& key, ReadBuffer& value)
{
    StorageKeyValue*    kv;
    
    kv = keys.Get<ReadBuffer&>(key);
    if (kv != NULL)
    {
        value = kv->value;
        return true;
    }   
    
    return false;
}

bool StorageDataPage::Set(ReadBuffer& key, ReadBuffer& value, bool copy)
{
    StorageKeyValue*    it;
    StorageKeyValue*    newStorageKeyValue;

    int res;
    it = keys.Locate<ReadBuffer&>(key, res);
    if (res == 0 && it != NULL)
    {
        if (BUFCMP(&value, &it->value))
            return false;
        // found
        required -= it->value.GetLength();
        it->SetValue(value, copy);
        required += it->value.GetLength();
        return true;
    }

    // not found
    newStorageKeyValue = new StorageKeyValue;
    newStorageKeyValue->SetKey(key, copy);
    newStorageKeyValue->SetValue(value, copy);
    keys.InsertAt(newStorageKeyValue, it, res);
    required += (DATAPAGE_KV_OVERHEAD + key.GetLength() + value.GetLength());
        
    return true;
}

void StorageDataPage::Delete(ReadBuffer& key)
{
    StorageKeyValue*    it;

    it = keys.Get<ReadBuffer&>(key);
    if (it)
    {
        keys.Remove(it);
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

StorageKeyValue* StorageDataPage::CursorBegin(ReadBuffer& key)
{
    StorageKeyValue*    it;
    int         retcmp;
    
    it = keys.Locate(key, retcmp);
    
    if (it == NULL)
        return NULL;
    
    if (retcmp <= 0)
        return it;
    else
        return keys.Next(it);
}

StorageKeyValue* StorageDataPage::CursorNext(StorageKeyValue* it)
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
    StorageDataPage*    newPage;
    StorageKeyValue*    it;
    StorageKeyValue*    next;
    uint32_t            target, sum, kvsize;
    
    if (required > 2 * pageSize)
        ASSERT_FAIL();
    
    target = (required - DATAPAGE_FIX_OVERHEAD) / 2 + DATAPAGE_FIX_OVERHEAD;
    sum = DATAPAGE_FIX_OVERHEAD;
    for (it = keys.First(); it != NULL; it = keys.Next(it))
    {
        kvsize = (it->key.GetLength() + it->value.GetLength() + DATAPAGE_KV_OVERHEAD);
        sum += kvsize;
        // if kvsize is greater than the target don't put it on a new page unless
        // there is no more space left on the current page
        if (sum > pageSize || ((kvsize + DATAPAGE_FIX_OVERHEAD) < target && sum > target))
            break;
    }
        
    assert(it != NULL);
    
    newPage = DCACHE->GetPage();
    DCACHE->Checkin(newPage);
    newPage->SetPageSize(pageSize);
    newPage->SetStorageFileIndex(fileIndex);
    newPage->SetFile(file);
    newPage->buffer.Allocate(pageSize);
    assert(newPage->required == DATAPAGE_FIX_OVERHEAD);
    assert(newPage->keys.GetCount() == 0);
    while (it != NULL)
    {
        required -= (DATAPAGE_KV_OVERHEAD + it->key.GetLength() + it->value.GetLength());
        // TODO: optimize buffer to avoid delete and realloc below
        next = keys.Next(it);
        keys.Remove(it);
        it->SetKey(it->key, true);
        it->SetValue(it->value, true);
        newPage->keys.Insert(it);
        newPage->required += (DATAPAGE_KV_OVERHEAD + it->key.GetLength() + it->value.GetLength());
        it = next;
    }
    
    // CHECK The next block is only needed when debugging to ensure that invariants are maintained
    unsigned r = DATAPAGE_FIX_OVERHEAD;
    unsigned num = 0;
    for (StorageKeyValue* it = newPage->keys.First(); it != NULL; it = newPage->keys.Next(it))
    {
        r += (DATAPAGE_KV_OVERHEAD + it->key.GetLength() + it->value.GetLength());
        num++;
    }   
    assert(num == newPage->keys.GetCount());
    assert(newPage->required == r);
    // CHECK block end

    assert(IsEmpty() != true);
    assert(newPage->IsEmpty() != true);
    
    return newPage;
}

StorageDataPage* StorageDataPage::SplitDataPageByKey(ReadBuffer& key)
{
    StorageDataPage*    newPage;
    StorageKeyValue*    it;
    StorageKeyValue*    next;

    newPage = DCACHE->GetPage();
    DCACHE->Checkin(newPage);
    newPage->SetPageSize(pageSize);
    newPage->SetStorageFileIndex(fileIndex);
    newPage->SetFile(file);
    newPage->buffer.Allocate(pageSize);
    
    assert(newPage->required == DATAPAGE_FIX_OVERHEAD);
    assert(newPage->keys.GetCount() == 0);
    assert(keys.Get(key) != NULL);
    
    for (it = keys.Get(key); it != NULL; it = next)
    {
        required -= (DATAPAGE_KV_OVERHEAD + it->key.GetLength() + it->value.GetLength());
        // TODO: optimize buffer to avoid delete and realloc below
        next = keys.Next(it);
        keys.Remove(it);
        it->SetKey(it->key, true);
        it->SetValue(it->value, true);
        newPage->keys.Insert(it);
        newPage->required += (DATAPAGE_KV_OVERHEAD + it->key.GetLength() + it->value.GetLength());
        it = next;
    }

    return newPage;
}

bool StorageDataPage::HasCursors()
{
    return (cursors.GetLength() > 0);
}

void StorageDataPage::Detach()
{
    StorageDataPage*    detached;
    StorageCursor*      cursor;
    StorageKeyValue*    it;
    StorageKeyValue*    kv;
    int                 cmpres;
    
    detached = new StorageDataPage(true);
    
    detached->cursors = cursors;
    cursors.ClearMembers();
    
    for (it = keys.First(); it != NULL; it = keys.Next(it))
    {
        kv = new StorageKeyValue;
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

void StorageDataPage::SetFile(StorageFile* file_)
{
    file = file_;
}

void StorageDataPage::Read(ReadBuffer& buffer_)
{
    uint32_t            num, len, i;
    StorageKeyValue*    kv;
    ReadBuffer          readBuffer;
    bool                ret;

    buffer.Allocate(pageSize);
    buffer.Write(buffer_);
    
    readBuffer = buffer;

    ret = readBuffer.ReadLittle32(pageSize);
    assert(ret == true);
    assert(pageSize != 0);

    readBuffer.Advance(sizeof(uint32_t));
    ret = readBuffer.ReadLittle32(fileIndex);
    assert(ret == true);

    readBuffer.Advance(sizeof(uint32_t));
    ret = readBuffer.ReadLittle32(offset);
    assert(ret == true);

    readBuffer.Advance(sizeof(uint32_t));
    ret = readBuffer.ReadLittle32(num);
    assert(ret == true);

    readBuffer.Advance(sizeof(uint32_t));
    STORAGE_TRACE("reading datapage for file %u at offset %u\n", fileIndex, offset);
    for (i = 0; i < num; i++)
    {
        kv = new StorageKeyValue;

        ret = readBuffer.ReadLittle32(len);
        assert(ret == true);
        readBuffer.Advance(sizeof(uint32_t));
        kv->key.SetLength(len);
        kv->key.SetBuffer(readBuffer.GetBuffer());

        readBuffer.Advance(len);
        ret = readBuffer.ReadLittle32(len);
        assert(ret == true);

        readBuffer.Advance(sizeof(uint32_t));
        kv->value.SetLength(len);
        kv->value.SetBuffer(readBuffer.GetBuffer());
        
        readBuffer.Advance(len);
        STORAGE_TRACE("read %.*s => %.*s\n", P(&(kv->key)), P(&(kv->value)));
        keys.Insert(kv);
    }
    
    required = readBuffer.GetBuffer() - buffer.GetBuffer();
    this->buffer.SetLength(required);
    assert(IsEmpty() != true);
}

bool StorageDataPage::CheckWrite(Buffer& buffer)
{
    StorageKeyValue*    it;
    unsigned            len;
    uint32_t            num;
    
    if (newPage)
        assert(this->buffer.GetLength() == 0);

    this->buffer.Allocate(pageSize);

    buffer.SetLength(0);

    assert(pageSize > 0);
    buffer.AppendLittle32(pageSize);

    assert(fileIndex != 0);
    buffer.AppendLittle32(fileIndex);

    buffer.AppendLittle32(offset);

    num = keys.GetCount();
    buffer.AppendLittle32(num);

//  printf("writing datapage for file %u at offset %u\n", fileIndex, offset);
    for (it = keys.First(); it != NULL; it = keys.Next(it))
    {
        len = it->key.GetLength();
        buffer.AppendLittle32(len);
        buffer.Append(it->key.GetBuffer(), it->key.GetLength());

        len = it->value.GetLength();
        buffer.AppendLittle32(len);
        buffer.Append(it->value.GetBuffer(), it->value.GetLength());
//      printf("writing %.*s => %.*s\n", P(&(it->key)), P(&(it->value)));
    }
    assert(required == buffer.GetLength());
    if (BUFCMP(&buffer, &this->buffer))
        return false;
    
//  for (it = keys.First(); it != NULL; it = keys.Next(it))
//  {
//      printf("written %.*s => %.*s\n", P(&(it->key)), P(&(it->value)));
//  }
    
    return true;
}

bool StorageDataPage::Write(Buffer& buffer)
{
    StorageKeyValue*    it;
    unsigned            len;
    unsigned            tmpLen;
    uint32_t            num;
    
    if (newPage)
        assert(this->buffer.GetLength() == 0);

    this->buffer.Allocate(pageSize);

    buffer.SetLength(0);

    assert(pageSize > 0);
    buffer.AppendLittle32(pageSize);

    assert(fileIndex != 0);
    buffer.AppendLittle32(fileIndex);

    buffer.AppendLittle32(offset);

    num = keys.GetCount();
    buffer.AppendLittle32(num);

//  printf("writing datapage for file %u at offset %u\n", fileIndex, offset);
    for (it = keys.First(); it != NULL; it = keys.Next(it))
    {
        len = it->key.GetLength();
        buffer.AppendLittle32(len);

        tmpLen = buffer.GetLength();
        buffer.Append(it->key.GetBuffer(), it->key.GetLength());

        if (it->keyBuffer)
        {
            delete it->keyBuffer;
            it->keyBuffer = NULL;
        }
        it->key.SetBuffer(this->buffer.GetBuffer() + tmpLen);

        len = it->value.GetLength();
        buffer.AppendLittle32(len);

        tmpLen = buffer.GetLength();
        buffer.Append(it->value.GetBuffer(), it->value.GetLength());

        if (it->valueBuffer)
        {
            delete it->valueBuffer;
            it->valueBuffer = NULL;
        }
        it->value.SetBuffer(this->buffer.GetBuffer() + tmpLen);

//      printf("writing %.*s => %.*s\n", P(&(it->key)), P(&(it->value)));
    }
    assert(required == buffer.GetLength());
    if (BUFCMP(&buffer, &this->buffer))
        return false;
    
//  for (it = keys.First(); it != NULL; it = keys.Next(it))
//  {
//      printf("written %.*s => %.*s\n", P(&(it->key)), P(&(it->value)));
//  }
    
    this->buffer.Write(buffer);
    
    return true;
}

void StorageDataPage::Invalidate()
{
    buffer.Clear();
}
