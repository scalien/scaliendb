#include "StorageMemoKeyValue.h"

StorageMemoKeyValue::StorageMemoKeyValue()
{
    keyBuffer = NULL;
    valueBuffer = NULL;
}

StorageMemoKeyValue::~StorageMemoKeyValue()
{
    if (keyBuffer != NULL)
        delete keyBuffer;
    if (valueBuffer != NULL)
        delete valueBuffer;
}

void StorageMemoKeyValue::Set(ReadBuffer key_, ReadBuffer value_)
{
    if (keyBuffer == NULL)
        keyBuffer = new Buffer;
    keyBuffer->Write(key_);
    key.Wrap(keyBuffer->GetBuffer(), keyBuffer->GetLength());
    if (valueBuffer == NULL)
        valueBuffer = new Buffer;
    valueBuffer->Write(value_);
    value.Wrap(valueBuffer->GetBuffer(), valueBuffer->GetLength());
}

void StorageMemoKeyValue::Delete(ReadBuffer key_)
{
    if (keyBuffer == NULL)
        keyBuffer = new Buffer;
    keyBuffer->Write(key_);
    key.Wrap(keyBuffer->GetBuffer(), keyBuffer->GetLength());
    if (valueBuffer != NULL)
        delete valueBuffer;
    valueBuffer = NULL;
    value.SetLength(0);
}

char StorageMemoKeyValue::GetType()
{
    if (valueBuffer != NULL)
        return STORAGE_KEYVALUE_TYPE_SET;
    else
        return STORAGE_KEYVALUE_TYPE_DELETE;
}

ReadBuffer& StorageMemoKeyValue::GetKey()
{
    return key;
}

ReadBuffer& StorageMemoKeyValue::GetValue()
{
    return value;
}

uint32_t StorageMemoKeyValue::GetLength()
{
    uint32_t length;

    length = 0;

    if (keyBuffer != NULL)
        length += keyBuffer->GetLength();
    if (valueBuffer != NULL)
        length += valueBuffer->GetLength();

    return length;
}
