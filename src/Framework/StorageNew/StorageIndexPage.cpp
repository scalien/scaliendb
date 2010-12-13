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
    size = 0;

    buffer.Allocate(STORAGE_DEFAULT_INDEX_PAGE_GRAN);
    buffer.Zero();

    buffer.AppendLittle32(0); // dummy for size
    buffer.AppendLittle32(0); // dummy for checksum
    buffer.AppendLittle32(0); // fummy for numKeys
}

uint32_t StorageIndexPage::GetSize()
{
    return size;
}

bool StorageIndexPage::Locate(ReadBuffer& key, uint32_t& index, uint32_t& offset)
{
    StorageIndexRecord* it;

    int cmpres;
    
    if (indexTree.GetCount() == 0)
        return false;
        
    if (ReadBuffer::LessThan(key, indexTree.First()->key))
        return false;
    
    it = indexTree.Locate<ReadBuffer&>(key, cmpres);
    if (cmpres >= 0)
    {
        index = it->index;
        offset = it->offset;
    }
    else
    {
        index = indexTree.Prev(it)->index;
        offset = indexTree.Prev(it)->offset;
    }

    return true;
}

void StorageIndexPage::Append(ReadBuffer key, uint32_t index, uint32_t offset)
{
    StorageIndexRecord* record;
    unsigned            keypos;

    buffer.AppendLittle32(offset);
    buffer.AppendLittle16(key.GetLength());
    keypos = buffer.GetLength();
    buffer.Append(key);
    
    record = new StorageIndexRecord;
    record->key = ReadBuffer(buffer.GetBuffer() + keypos, key.GetLength());
    record->index = index;
    record->offset = offset;
    
    indexTree.Insert(record);
}

void StorageIndexPage::Finalize()
{
    uint32_t            div, mod, numKeys, checksum, length;
    ReadBuffer          dataPart;

    numKeys = indexTree.GetCount();
    length = buffer.GetLength();

    div = length / STORAGE_DEFAULT_INDEX_PAGE_GRAN;
    mod = length % STORAGE_DEFAULT_INDEX_PAGE_GRAN;
    size = div * STORAGE_DEFAULT_INDEX_PAGE_GRAN;
    if (mod > 0)
        size += STORAGE_DEFAULT_INDEX_PAGE_GRAN;

    buffer.Allocate(size);
    buffer.Zero();

    // compute checksum
    dataPart.SetBuffer(buffer.GetBuffer() + 8);
    dataPart.SetLength(size - 8);
    checksum = dataPart.GetChecksum();

    buffer.SetLength(0);
    buffer.AppendLittle32(size);
    buffer.AppendLittle32(checksum);
    buffer.AppendLittle32(numKeys);

    buffer.SetLength(size);
}

void StorageIndexPage::Write(Buffer& writeBuffer)
{
    writeBuffer.Write(buffer);
}
