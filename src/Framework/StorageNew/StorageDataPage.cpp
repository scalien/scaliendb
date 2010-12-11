#include "StorageDataPage.h"

static int KeyCmp(const ReadBuffer a, const ReadBuffer b)
{
    return ReadBuffer::Cmp(a, b);
}

static const ReadBuffer Key(StorageKeyValue* kv)
{
    return kv->GetKey();
}


StorageDataPage::StorageDataPage()
{
    length = 0;
    length += 4; // pageSize
    length += 4; // checksum
    length += 4; // numKeys
    size = 0;
}

uint32_t StorageDataPage::GetSize()
{
    return size;
}
uint32_t StorageDataPage::GetNumKeys()
{
    return keyValues.GetCount();
}

uint32_t StorageDataPage::GetLength()
{
    return length;
}

uint32_t StorageDataPage::GetIncrement(StorageKeyValue* kv)
{
    if (kv->GetType() == STORAGE_KEYVALUE_TYPE_SET)
        return (1 + 2 + kv->GetKey().GetLength() + 4 + kv->GetValue().GetLength());
    else if (kv->GetType() == STORAGE_KEYVALUE_TYPE_DELETE)
        return (1 + 2 + kv->GetKey().GetLength());
    else
        ASSERT_FAIL();

    return 0;
}

void StorageDataPage::Append(StorageKeyValue* kv_)
{
    StorageKeyValue* kv;
    
    kv = new StorageKeyValue;
    *kv = *kv_;
    
    keyValues.Insert(kv);

    length += GetIncrement(kv);
}

void StorageDataPage::Finalize()
{
    uint32_t            div, mod, numKeys, checksum;
    ReadBuffer          dataPart;
    StorageKeyValue*    it;

    numKeys = keyValues.GetCount();

    div = length / STORAGE_DEFAULT_DATA_PAGE_GRAN;
    mod = length % STORAGE_DEFAULT_DATA_PAGE_GRAN;
    size = div * STORAGE_DEFAULT_DATA_PAGE_GRAN;
    if (mod > 0)
        size += STORAGE_DEFAULT_DATA_PAGE_GRAN;

    buffer.Allocate(size);
    buffer.Zero();

    buffer.AppendLittle32(size);
    buffer.AppendLittle32(0); // dummy for checksum
    buffer.AppendLittle32(numKeys);

    FOREACH(it, keyValues)
    {
        if (it->GetType() == STORAGE_KEYVALUE_TYPE_SET)
        {
            buffer.Append(STORAGE_KEYVALUE_TYPE_SET);
            buffer.AppendLittle16(it->GetKey().GetLength());
            buffer.Append(it->GetKey());
        }
        else if (it->GetType() == STORAGE_KEYVALUE_TYPE_DELETE)
        {
            buffer.Append(STORAGE_KEYVALUE_TYPE_SET);
            buffer.AppendLittle16(it->GetKey().GetLength());
            buffer.Append(it->GetKey());
            buffer.AppendLittle32(it->GetValue().GetLength());
            buffer.Append(it->GetValue());
        }
        else
            ASSERT_FAIL();
    }

    assert(length == buffer.GetLength());

    // now write checksum
    dataPart.SetBuffer(buffer.GetBuffer() + 8);
    dataPart.SetLength(size - 8);
    checksum = dataPart.GetChecksum();

    buffer.SetLength(4);
    buffer.AppendLittle32(checksum);
    buffer.SetLength(size);
}

void StorageDataPage::Write(Buffer& writeBuffer)
{
    writeBuffer.Write(buffer);
}
