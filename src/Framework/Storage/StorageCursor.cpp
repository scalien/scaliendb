#include "StorageCursor.h"
#include "StorageCatalog.h"

StorageCursor::StorageCursor(StorageCatalog* catalog_)
{
	catalog = catalog_;
	dataPage = NULL;
	current = NULL;
}

KeyValue* StorageCursor::Begin(ReadBuffer& key)
{
	nextKey.Clear();
	dataPage = catalog->CursorBegin(key, nextKey); // sets nextKey
	
	if (dataPage == NULL)
		return NULL;
	
	dataPage->RegisterCursor();
	
	current = dataPage->BeginIteration(key);
	if (current != NULL)
		return current;
	
	return FromNextPage();
}

KeyValue* StorageCursor::Next()
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
		dataPage->UnregisterCursor();
}

KeyValue* StorageCursor::FromNextPage()
{
	Buffer			buffer;
	ReadBuffer		key;

	dataPage->UnregisterCursor();
	
	// attempt to advance to next dataPage
	if (nextKey.GetLength() == 0)
		return NULL; // we're at the end
	
	// advance
	buffer.Write(nextKey);
	key.Wrap(buffer);
	return Begin(key);
}