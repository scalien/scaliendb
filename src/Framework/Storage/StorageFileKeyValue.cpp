#include "StorageFileKeyValue.h"

StorageFileKeyValue::StorageFileKeyValue()
{
    type = 0;
}

void StorageFileKeyValue::Set(ReadBuffer key_, ReadBuffer value_)
{
    type = STORAGE_KEYVALUE_TYPE_SET;
    key = key_;
    value = value_;
}

void StorageFileKeyValue::Delete(ReadBuffer key_)
{
    type = STORAGE_KEYVALUE_TYPE_DELETE;
    key = key_;
}

char StorageFileKeyValue::GetType()
{
    if (type == 0)
        ASSERT_FAIL();

    return type;
}

ReadBuffer StorageFileKeyValue::GetKey() const
{
    return key;
}

const ReadBuffer& StorageFileKeyValue::GetKeyReference() const
{
    return key;
}


ReadBuffer StorageFileKeyValue::GetValue() const
{
    return value;
}
