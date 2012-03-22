#include "StorageShardProxy.h"
#include "StorageEnvironment.h"

void StorageShardProxy::Init(StorageEnvironment* environment_, 
 uint16_t contextID_,
 uint64_t shardID_)
{
    environment = environment_;
    contextID = contextID_;
    shardID = shardID_;
    trackID = environment->GetShard(contextID, shardID)->GetTrackID();
}

bool StorageShardProxy::Get(ReadBuffer key, ReadBuffer& value)
{
    return environment->Get(contextID, shardID, key, value);
}

bool StorageShardProxy::Set(ReadBuffer key, ReadBuffer value)
{
    return environment->Set(contextID, shardID, key, value);
}

bool StorageShardProxy::Delete(ReadBuffer key)
{
    return environment->Delete(contextID, shardID, key);
}

StorageEnvironment* StorageShardProxy::GetEnvironment()
{
    return environment;
}

uint16_t StorageShardProxy::GetContextID()
{
    return contextID;
}

uint64_t StorageShardProxy::GetShardID()
{
    return shardID;
}

uint64_t StorageShardProxy::GetTrackID()
{
    return trackID;
}