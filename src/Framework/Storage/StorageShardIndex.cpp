#include "StorageShardIndex.h"
#include "StorageShard.h"

StorageShardIndex::StorageShardIndex()
{
	shard = NULL;
	startKeyBuffer = NULL;
	endKeyBuffer = NULL;
}

StorageShardIndex::~StorageShardIndex()
{
	delete shard;
	delete startKeyBuffer;
	delete endKeyBuffer;
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

void StorageShardIndex::SetEndKey(ReadBuffer key_, bool copy)
{
	if (endKeyBuffer != NULL && !copy)
	{
		delete endKeyBuffer;
		endKeyBuffer = NULL;
	}
	
	if (copy)
	{
		if (endKeyBuffer == NULL)
			endKeyBuffer = new Buffer; // TODO: allocation strategy
		endKeyBuffer->Write(key_);
		endKey.Wrap(*endKeyBuffer);
	}
	else
		endKey = key_;
}

