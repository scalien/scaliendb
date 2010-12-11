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
    length = 4; // numKeys
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
    unsigned            div, mod;
    StorageKeyValue*    it;

    buffer.AppendLittle32(keyValues.GetCount());
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

    div = length / STORAGE_DEFAULT_DATA_PAGE_GRAN;
    mod = length % STORAGE_DEFAULT_DATA_PAGE_GRAN;
    size = div * STORAGE_DEFAULT_DATA_PAGE_GRAN;
    if (mod > 0)
        size += STORAGE_DEFAULT_DATA_PAGE_GRAN;

    buffer.Allocate(size);
}
