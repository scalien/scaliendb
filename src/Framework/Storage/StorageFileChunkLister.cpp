#include "StorageFileChunkLister.h"

#define MAX_PRELOAD_THRESHOLD   1*MB

void StorageFileChunkLister::SetFilename(ReadBuffer filename_)
{
    filename.Write(filename_);
}

void StorageFileChunkLister::Load(bool keysOnly)
{
    reader.Open(filename, MAX_PRELOAD_THRESHOLD, keysOnly);
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
