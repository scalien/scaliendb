#include "StorageKeyValue.h"

StorageKeyValue::StorageKeyValue()
{
    keyBuffer = NULL;
    valueBuffer = NULL;
}

StorageKeyValue::~StorageKeyValue()
{
    if (keyBuffer != NULL)
        delete keyBuffer;
    if (valueBuffer != NULL)
        delete valueBuffer;
}

void StorageKeyValue::SetKey(ReadBuffer& key_, bool copy)
{
    if (keyBuffer != NULL && !copy)
    {
        delete keyBuffer;
        keyBuffer = NULL;
    }
    
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

void StorageKeyValue::SetValue(ReadBuffer& value_, bool copy)
{
    if (valueBuffer != NULL && !copy)
    {
        delete valueBuffer;
        valueBuffer = NULL;
    }

    if (copy)
    {
        if (valueBuffer == NULL)
            valueBuffer = new Buffer; // TODO: allocation strategy
        valueBuffer->Write(value_);
        value.Wrap(*valueBuffer);
    }
    else
        value = value_;
}
