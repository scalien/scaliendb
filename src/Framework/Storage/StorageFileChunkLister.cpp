#include "StorageFileChunkLister.h"

#define MAX_PRELOAD_THRESHOLD   1*MB

void StorageFileChunkLister::Load(ReadBuffer filename)
{
    reader.Open(filename, MAX_PRELOAD_THRESHOLD);
}

StorageFileKeyValue* StorageFileChunkLister::First(ReadBuffer& firstKey)
{
    return reader.First(firstKey);
}

StorageFileKeyValue* StorageFileChunkLister::Next(StorageFileKeyValue* kv)
{
    return reader.Next(kv);
}

uint64_t StorageFileChunkLister::GetNumKeys()
{
    return reader.GetNumKeys();
}
