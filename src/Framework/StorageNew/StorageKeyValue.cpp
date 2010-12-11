#include "StorageKeyValue.h"

StorageMemoKeyValue::StorageMemoKeyValue()
{
    key = NULL;
    value = NULL;
}

void StorageMemoKeyValue::Set(ReadBuffer& key_, ReadBuffer& value_)
{
    if (key == NULL)
        key = new Buffer;
    key->Write(key_);
    if (value == NULL)
        value = new Buffer;
    value->Write(value_);
}

void StorageMemoKeyValue::Delete(ReadBuffer& key_)
{
    if (key == NULL)
        key = new Buffer;
    key->Write(key_);
    if (value != NULL)
        delete value;
    value = NULL;
}

char StorageMemoKeyValue::GetType()
{
    if (value != NULL)
        return STORAGE_KEYVALUE_TYPE_SET;
    else
        return STORAGE_KEYVALUE_TYPE_DELETE;
}

ReadBuffer StorageMemoKeyValue::GetKey()
{
    return ReadBuffer(*key);
}

ReadBuffer StorageMemoKeyValue::GetValue()
{
    return ReadBuffer(*value);
}

uint32_t StorageMemoKeyValue::GetLength()
{
    uint32_t length;

    length = 0;

    if (key != NULL)
        length += key->GetLength();
    if (value != NULL)
        length += value->GetLength();

    return length;
}
