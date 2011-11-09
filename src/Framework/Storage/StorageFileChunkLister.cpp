#include "StorageFileChunkLister.h"

void StorageFileChunkLister::Init(
 StorageFileChunk* fileChunk_,
 ReadBuffer firstKey_, ReadBuffer endKey_, ReadBuffer prefix_, unsigned count_, bool keysOnly_,
 uint64_t preloadBufferSize_, bool forwardDirection_)
{
    firstKey = firstKey_;
    endKey = endKey_;
    prefix = prefix_;
    count = count_;
    reader.OpenWithFileChunk(fileChunk_, preloadBufferSize_, keysOnly_, forwardDirection_);
    reader.SetPrefix(prefix);
    reader.SetEndKey(endKey);
    reader.SetCount(count);
    Log_Debug("FileChunkLister preloadBufferSize: %s", HUMAN_BYTES(preloadBufferSize_));
}

void StorageFileChunkLister::Load()
{
}

void StorageFileChunkLister::SetDirection(bool forwardDirection_)
{
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
