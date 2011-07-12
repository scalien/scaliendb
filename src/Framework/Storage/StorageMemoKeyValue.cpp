#include "StorageMemoKeyValue.h"
#include "StorageMemoChunk.h"

#define DELETE_LENGTH_VALUE     ((uint32_t)-1)

StorageMemoKeyValue::StorageMemoKeyValue()
{
    buffer = NULL;
    keyLength = 0;
    valueLength = 0;
}

StorageMemoKeyValue::~StorageMemoKeyValue()
{
    // NOTE: buffer is deallocated in StorageMemoChunk!
}

void StorageMemoKeyValue::Free(StorageMemoChunk* memoChunk)
{
    if (buffer != NULL)
    {
        memoChunk->Free(this, buffer);
        buffer = NULL;
    }
}

void StorageMemoKeyValue::Set(ReadBuffer key_, ReadBuffer value_, StorageMemoChunk* memoChunk)
{
    if (buffer != NULL && GetLength() < key_.GetLength() + value_.GetLength())
    {
        memoChunk->Free(this, buffer);
        buffer = NULL;
    }
    
    keyLength = key_.GetLength();
    valueLength = value_.GetLength();

    if (buffer == NULL)
        buffer = (char*) memoChunk->Alloc(this, GetLength());
    
    memcpy(buffer, key_.GetBuffer(), keyLength);
    memcpy(buffer + keyLength, value_.GetBuffer(), valueLength);
}

void StorageMemoKeyValue::Delete(ReadBuffer key_, StorageMemoChunk* memoChunk)
{
    //if (buffer != NULL)
    //    memoChunk->Free(this, buffer);


    keyLength = key_.GetLength();
    valueLength = DELETE_LENGTH_VALUE;

    //buffer = (char*) memoChunk->Alloc(this, GetLength());
    
    if (buffer == NULL)
    {
        buffer = (char*) memoChunk->Alloc(this, GetLength());
        memcpy(buffer, key_.GetBuffer(), keyLength);
    }
}

char StorageMemoKeyValue::GetType()
{
    if (valueLength == DELETE_LENGTH_VALUE)
        return STORAGE_KEYVALUE_TYPE_DELETE;
    else
        return STORAGE_KEYVALUE_TYPE_SET;
}

ReadBuffer StorageMemoKeyValue::GetKey() const
{
    return ReadBuffer(buffer, keyLength);
}

ReadBuffer StorageMemoKeyValue::GetValue() const
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
