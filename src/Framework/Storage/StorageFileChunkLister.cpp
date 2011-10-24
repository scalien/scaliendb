#include "StorageFileChunkLister.h"

void StorageFileChunkLister::Init(
 ReadBuffer filename_, ReadBuffer firstKey_, ReadBuffer endKey_, ReadBuffer prefix_, unsigned count_, bool keysOnly_,
 uint64_t preloadBufferSize_, bool forwardDirection_)
{
    filename.Write(filename_);
    firstKey = firstKey_;
    endKey = endKey_;
    prefix = prefix_;
    count = count_;
    keysOnly = keysOnly_;
    preloadBufferSize = preloadBufferSize_;
    forwardDirection = forwardDirection_;
    Log_Debug("FileChunkLister preloadBufferSize: %s", HUMAN_BYTES(preloadBufferSize));
}

void StorageFileChunkLister::Load()
{
    reader.Open(filename, preloadBufferSize, keysOnly, forwardDirection);
    reader.SetPrefix(prefix);
    reader.SetEndKey(endKey);
    reader.SetCount(count);
}

void StorageFileChunkLister::SetDirection(bool forwardDirection_)
{
    ASSERT(forwardDirection == forwardDirection_);
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
