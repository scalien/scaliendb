#include "StorageCursor.h"
#include "StorageTable.h"
#include "StorageShard.h"

StorageCursor::StorageCursor(StorageTable* table_)
{
    table = table_;
    shard = NULL;
    file = NULL;
    dataPage = NULL;
    current = NULL;
    prev = next = this;
}

StorageCursor::StorageCursor(StorageShard* shard_)
{
    table = NULL;
    shard = shard_;
    file = NULL;
    dataPage = NULL;
    current = NULL;
    prev = next = this;
}

void StorageCursor::SetBulk(bool bulk_)
{
    bulk = bulk_;
}

StorageKeyValue* StorageCursor::Begin()
{
    return Begin(ReadBuffer());
}

StorageKeyValue* StorageCursor::Begin(ReadBuffer key)
{
    nextKey.Clear();
    if (table)
        dataPage = table->CursorBegin(this, key);      // sets nextKey
    else
        dataPage = shard->CursorBegin(this, key);      // sets nextKey
    
    if (dataPage == NULL)
        return NULL;
    
    dataPage->RegisterCursor(this);
    
    current = dataPage->CursorBegin(key);
    if (current != NULL)
        return current;
    
    return FromNextPage();
}

StorageKeyValue* StorageCursor::Next()
{
    assert(current != NULL);
    
    current = dataPage->CursorNext(current);
    
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
    Buffer          buffer;
    ReadBuffer      key;

    dataPage->UnregisterCursor(this);
    
    // attempt to advance to next dataPage
    if (nextKey.GetLength() == 0)
        return NULL; // we're at the end
    
    // advance
    buffer.Write(nextKey);
    key.Wrap(buffer);
    return Begin(key);
}