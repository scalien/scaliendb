#include "StorageCursor.h"
#include "StorageShard.h"

StorageCursor::StorageCursor(StorageShard* shard_)
{
	shard = shard_;
	dataPage = NULL;
	current = NULL;
}

StorageKeyValue* StorageCursor::Begin(ReadBuffer& key)
{
	nextKey.Clear();
	dataPage = shard->CursorBegin(key, nextKey); // sets nextKey
	
	if (dataPage == NULL)
		return NULL;
	
	dataPage->RegisterCursor(this);
	
	current = dataPage->BeginIteration(key);
	if (current != NULL)
		return current;
	
	return FromNextPage();
}

StorageKeyValue* StorageCursor::Next()
{
	assert(current != NULL);
	
	current = dataPage->Next(current);
	
	if (current != NULL)
		return current;
	
	return FromNextPage();
}

void StorageCursor::Close()
{
	if (dataPage)
		dataPage->UnregisterCursor(this);
	
	delete this;
}

StorageKeyValue* StorageCursor::FromNextPage()
{
	Buffer			buffer;
	ReadBuffer		key;

	dataPage->UnregisterCursor(this);
	
	// attempt to advance to next dataPage
	if (nextKey.GetLength() == 0)
		return NULL; // we're at the end
	
	// advance
	buffer.Write(nextKey);
	key.Wrap(buffer);
	return Begin(key);
}