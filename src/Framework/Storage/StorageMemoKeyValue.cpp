#include "StorageMemoKeyValue.h"

#define DELETE_LENGTH_VALUE     ((uint32_t)-1)

StorageMemoKeyValue::StorageMemoKeyValue()
{
    buffer = NULL;
    keyLength = 0;
    valueLength = 0;
}

StorageMemoKeyValue::~StorageMemoKeyValue()
{
    free(buffer);
}

void StorageMemoKeyValue::Set(ReadBuffer key_, ReadBuffer value_)
{
    if (buffer != NULL)
        free(buffer);
    
    keyLength = key_.GetLength();
    valueLength = value_.GetLength();

    buffer = (char*) malloc(keyLength + valueLength);
    
    memcpy(buffer, key_.GetBuffer(), keyLength);
    memcpy(buffer + keyLength, value_.GetBuffer(), valueLength);
}

void StorageMemoKeyValue::Delete(ReadBuffer key_)
{
    if (buffer != NULL)
        free(buffer);
    
    keyLength = key_.GetLength();
    valueLength = DELETE_LENGTH_VALUE;

    buffer = (char*) malloc(keyLength + valueLength);
    
    memcpy(buffer, key_.GetBuffer(), keyLength);
}

char StorageMemoKeyValue::GetType()
{
    if (valueLength == DELETE_LENGTH_VALUE)
        return STORAGE_KEYVALUE_TYPE_DELETE;
    else
        return STORAGE_KEYVALUE_TYPE_SET;
}

ReadBuffer StorageMemoKeyValue::GetKey()
{
    return ReadBuffer(buffer, keyLength);
}

ReadBuffer StorageMemoKeyValue::GetValue()
{
    if (valueLength == DELETE_LENGTH_VALUE)
        return ReadBuffer();
    else
        return ReadBuffer(buffer + keyLength, valueLength);
}

uint32_t StorageMemoKeyValue::GetLength()
{
    if (valueLength == DELETE_LENGTH_VALUE)
        return keyLength;
    else
        return keyLength + valueLength;
}
