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
}

uint32_t StorageDataPage::GetSize()
{
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
        return 1 + 2 + kv->GetKey().GetLength() + 2 + kv->GetValue().GetLength();
    else
        return 1 + 2 + kv->GetKey().GetLength();
}

void StorageDataPage::Append(StorageKeyValue* kv_)
{
    StorageKeyValue* kv;
    
    kv = new StorageKeyValue;
    *kv = *kv_;
    
    keyValues.Insert(kv);
}

void StorageDataPage::Finalize()
{
}
