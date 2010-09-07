#include "StorageFileIndex.h"

StorageFileIndex::StorageFileIndex()
{
	file = NULL;
	keyBuffer = NULL;
}

StorageFileIndex::~StorageFileIndex()
{
	if (keyBuffer != NULL)
		delete keyBuffer;
	delete file;
}

void StorageFileIndex::SetKey(ReadBuffer key_, bool copy)
{
	if (keyBuffer != NULL && !copy)
	{
		delete keyBuffer;
		keyBuffer = NULL;
	}
	
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
