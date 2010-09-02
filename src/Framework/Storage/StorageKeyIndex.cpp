#include "StorageKeyIndex.h"

StorageKeyIndex::StorageKeyIndex()
{
	keyBuffer = NULL;
}

StorageKeyIndex::~StorageKeyIndex()
{
	if (keyBuffer != NULL)
		delete keyBuffer;
}

void StorageKeyIndex::SetKey(ReadBuffer& key_, bool copy)
{
	if (keyBuffer != NULL && !copy)
		delete keyBuffer;
	
	if (copy)
	{
		if (keyBuffer == NULL)
			keyBuffer = new Buffer; // TODO: allocation strategy
		keyBuffer->Write(key_);
		key.Wrap(*keyBuffer);
	}
	else
		key = key_;
}
