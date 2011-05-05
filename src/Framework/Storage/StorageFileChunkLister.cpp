#include "StorageFileChunkLister.h"

void StorageFileChunkLister::Init(ReadBuffer filename_, bool keysOnly_, uint64_t preloadBufferSize_)
{
    filename.Write(filename_);
    keysOnly = keysOnly_;
    preloadBufferSize = preloadBufferSize_;
    Log_Debug("FileChunkLister preloadBufferSize: %s", HUMAN_BYTES(preloadBufferSize));
}

void StorageFileChunkLister::Load()
{
    reader.Open(filename, preloadBufferSize, keysOnly);
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
