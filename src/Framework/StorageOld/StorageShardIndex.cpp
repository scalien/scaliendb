#include "StorageShardIndex.h"
#include "StorageShard.h"

StorageShardIndex::StorageShardIndex()
{
    shard = NULL;
    startKeyBuffer = NULL;
}

StorageShardIndex::~StorageShardIndex()
{
    delete shard;
    delete startKeyBuffer;
}

void StorageShardIndex::SetStartKey(ReadBuffer key_, bool copy)
{
    if (startKeyBuffer != NULL && !copy)
    {
        delete startKeyBuffer;
        startKeyBuffer = NULL;
    }
    
    if (copy)
    {
        if (startKeyBuffer == NULL)
            startKeyBuffer = new Buffer; // TODO: allocation strategy
        startKeyBuffer->Write(key_);
        startKey.Wrap(*startKeyBuffer);
    }
    else
        startKey = key_;
}
