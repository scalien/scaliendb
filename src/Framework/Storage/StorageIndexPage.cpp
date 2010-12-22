#include "StorageIndexPage.h"
#include "StorageFileChunk.h"

static int KeyCmp(const ReadBuffer& a, const ReadBuffer& b)
{
    return ReadBuffer::Cmp(a, b);
}

static const ReadBuffer& Key(StorageIndexRecord* record)
{
    return record->key;
}

StorageIndexPage::StorageIndexPage(StorageFileChunk* owner_)
{
    size = 0;

    buffer.Allocate(STORAGE_DEFAULT_PAGE_GRAN);
    buffer.Zero();

    buffer.AppendLittle32(0); // dummy for size
    buffer.AppendLittle32(0); // dummy for checksum
    buffer.AppendLittle32(0); // dummy for numKeys
    
    owner = owner_;
}

uint32_t StorageIndexPage::GetSize()
{
    return size;
}

uint32_t StorageIndexPage::GetNumDataPages()
{
    return indexTree.GetCount();
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
        it = indexTree.Prev(it);
        index = it->index;
        offset = it->offset;
    }

    return true;
}

void StorageIndexPage::Append(ReadBuffer key, uint32_t index, uint32_t offset)
{
    StorageIndexRecord* record;

    buffer.AppendLittle32(offset);
    buffer.AppendLittle16(key.GetLength());
    buffer.Append(key);
    
    record = new StorageIndexRecord;
    record->key = key;
    record->index = index;
    record->offset = offset;
    
    indexTree.Insert(record);
}

void StorageIndexPage::Finalize()
{
    uint32_t            div, mod, numKeys, checksum, length, klen, pos;
    char*               kpos;
    ReadBuffer          dataPart;
    StorageIndexRecord* it;

    numKeys = indexTree.GetCount();
    length = buffer.GetLength();

    div = length / STORAGE_DEFAULT_PAGE_GRAN;
    mod = length % STORAGE_DEFAULT_PAGE_GRAN;
    size = div * STORAGE_DEFAULT_PAGE_GRAN;
    if (mod > 0)
        size += STORAGE_DEFAULT_PAGE_GRAN;

    buffer.Allocate(size);
    buffer.ZeroRest();

    // write numKeys
    buffer.SetLength(8);
    buffer.AppendLittle32(numKeys);

    // compute checksum
    dataPart.Wrap(buffer.GetBuffer() + 8, size - 8);
    checksum = dataPart.GetChecksum();

    buffer.SetLength(0);
    buffer.AppendLittle32(size);
    buffer.AppendLittle32(checksum);

    buffer.SetLength(size);

    // set ReadBuffers in tree
    pos = 12;
    FOREACH (it, indexTree)
    {
        klen = it->key.GetLength();
        
        pos += 4;                           // offset
        pos += 2;                           // klen
        kpos = buffer.GetBuffer() + pos;        
        it->key = ReadBuffer(kpos, klen);
        pos += klen;
    }
}

bool StorageIndexPage::Read(Buffer& buffer_)
{
    uint16_t                klen;
    uint32_t                size, checksum, compChecksum, numKeys, offset, i;
    ReadBuffer              dataPart, parse, key;
    StorageIndexRecord*     it;
    
    assert(indexTree.GetCount() == 0);
    
    buffer.Write(buffer_);
    parse.Wrap(buffer);
    
    // size
    parse.ReadLittle32(size);
    if (size < 12)
        goto Fail;
    if (buffer.GetLength() != size)
        goto Fail;
    parse.Advance(4);

    // checksum
    parse.ReadLittle32(checksum);
    dataPart.Wrap(buffer.GetBuffer() + 8, buffer.GetLength() - 8);
    compChecksum = dataPart.GetChecksum();
    if (compChecksum != checksum)
        goto Fail;
    parse.Advance(4);
    
    // numkeys
    parse.ReadLittle32(numKeys);
    parse.Advance(4);

    // keys
    for (i = 0; i < numKeys; i++)
    {
        // offset
        if (!parse.ReadLittle32(offset))
            goto Fail;
        parse.Advance(4);

        // klen
        if (!parse.ReadLittle16(klen))
            goto Fail;
        parse.Advance(2);
        
        // key
        if (parse.GetLength() < klen)
            goto Fail;
        key.Wrap(parse.GetBuffer(), klen);
        parse.Advance(klen);
        
        it = new StorageIndexRecord;
        it->key = key;
        it->index = i;
        it->offset = offset;
        indexTree.Insert(it);
    }
    
    this->size = size;
    return true;
    
Fail:
    indexTree.DeleteTree();
    buffer.Reset();
    return false;
}

void StorageIndexPage::Write(Buffer& buffer_)
{
    buffer_.Write(buffer);
}

void StorageIndexPage::Unload()
{
    indexTree.DeleteTree();
    buffer.Reset();
    owner->OnIndexPageEvicted();
}
