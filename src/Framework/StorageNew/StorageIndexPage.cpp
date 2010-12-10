#include "StorageIndexPage.h"

static int KeyCmp(const ReadBuffer& a, const ReadBuffer& b)
{
    return ReadBuffer::Cmp(a, b);
}

static const ReadBuffer& Key(StorageIndexRecord* record)
{
    return record->key;
}

uint32_t StorageIndexPage::GetSize()
{
}

void StorageIndexPage::Append(ReadBuffer& key, uint32_t index, uint32_t offset)
{
    StorageIndexRecord* record;
    
    record = new StorageIndexRecord;
    record->key = key;
    record->index = index;
    record->offset = offset;
    
    indexTree.Insert(record);
}
