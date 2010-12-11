#include "StorageIndexPage.h"

static int KeyCmp(const ReadBuffer& a, const ReadBuffer& b)
{
    return ReadBuffer::Cmp(a, b);
}

static const ReadBuffer& Key(StorageIndexRecord* record)
{
    return record->key;
}

StorageIndexPage::StorageIndexPage()
{
    length = 4; // numKeys
    size = 0;
}

uint32_t StorageIndexPage::GetSize()
{
    return size;
}

void StorageIndexPage::Append(ReadBuffer& key, uint32_t index, uint32_t offset)
{
    StorageIndexRecord* record;
    
    record = new StorageIndexRecord;
    record->key = key;
    record->index = index;
    record->offset = offset;
    
    indexTree.Insert(record);

    length += (4 + 2 + key.GetLength());
}

void StorageIndexPage::Finalize()
{
    unsigned            div, mod;
    StorageIndexRecord* it;

    buffer.AppendLittle32(indexTree.GetCount());
    FOREACH(it, indexTree)
    {
        buffer.AppendLittle32(it->offset);
        buffer.AppendLittle16(it->key.GetLength());
        buffer.Append(it->key);
    }

    assert(length == buffer.GetLength());

    div = length / STORAGE_DEFAULT_INDEX_PAGE_GRAN;
    mod = length % STORAGE_DEFAULT_INDEX_PAGE_GRAN;
    size = div * STORAGE_DEFAULT_INDEX_PAGE_GRAN;
    if (mod > 0)
        size += STORAGE_DEFAULT_INDEX_PAGE_GRAN;

    buffer.Allocate(size);
}
