#include "StorageIndexPage.h"
#include "StorageFileChunk.h"

#define STORAGE_INDEXPAGE_HEADER_SIZE   12

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

StorageIndexPage::~StorageIndexPage()
{
    indexTree.DeleteTree();
}

void StorageIndexPage::SetOwner(StorageFileChunk* owner_)
{
    ASSERT(owner == NULL);
    owner = owner_;
}

uint32_t StorageIndexPage::GetSize()
{
    return size;
}

uint32_t StorageIndexPage::GetMemorySize()
{
    return size + indexTree.GetCount() * sizeof(StorageIndexRecord);
}

uint32_t StorageIndexPage::GetNumDataPages()
{
    return indexTree.GetCount();
}

bool StorageIndexPage::Locate(ReadBuffer& key, uint32_t& index, uint64_t& offset)
{
    StorageIndexRecord* it;

    int cmpres;
    
    if (indexTree.GetCount() == 0)
        return false;
        
    if (ReadBuffer::LessThan(key, indexTree.First()->key))
        return false;
    
    it = indexTree.Locate(key, cmpres);
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

ReadBuffer StorageIndexPage::GetFirstKey()
{
    ASSERT(indexTree.First() != NULL);
    return indexTree.First()->key;
}

ReadBuffer StorageIndexPage::GetLastKey()
{
    ASSERT(indexTree.Last() != NULL);
    return indexTree.Last()->key;
}

ReadBuffer StorageIndexPage::GetMidpoint()
{
    if (midpoint.GetLength() > 0)
        return midpoint;
    return indexTree.Mid()->key;
}

ReadBuffer StorageIndexPage::GetIndexKey(uint32_t index)
{
    StorageIndexRecord* it;

    FOREACH (it, indexTree)
    {
        if (it->index == index)
            return it->key;
    }
    
    return ReadBuffer();
}

uint64_t StorageIndexPage::GetFirstDatapageOffset()
{
    ASSERT(indexTree.First() != NULL);
    return indexTree.First()->offset;
}

uint64_t StorageIndexPage::GetLastDatapageOffset()
{
    ASSERT(indexTree.Last() != NULL);
    return indexTree.Last()->offset;
}

uint32_t StorageIndexPage::GetOffsetIndex(uint64_t& offset)
{
    StorageIndexRecord* it;

    FOREACH(it, indexTree)
    {
        if (it->offset > offset)
        {
            if (it != indexTree.First())
                it = indexTree.Prev(it);
            offset = it->offset;
            return it->index;
        }
    }

    return indexTree.Last()->index;
}

uint64_t StorageIndexPage::GetIndexOffset(uint32_t index)
{
    StorageIndexRecord* it;

    FOREACH(it, indexTree)
    {
        if (it->index == index)
            return it->offset;
    }
    
    return 0;
}

void StorageIndexPage::Append(ReadBuffer key, uint32_t index, uint64_t offset)
{
    StorageIndexRecord* record;

    buffer.AppendLittle64(offset);
    buffer.AppendLittle16(key.GetLength());
    buffer.Append(key);
    
    record = new StorageIndexRecord;
    record->keyBuffer.Write(key);
    record->key.Wrap(record->keyBuffer);
    record->index = index;
    record->offset = offset;
    
    indexTree.Insert<const ReadBuffer&>(record);
}

void StorageIndexPage::Finalize()
{
    uint32_t            div, mod, numKeys, checksum, length, klen, pos;
    char*               kpos;
    ReadBuffer          dataPart;
    StorageIndexRecord* it;
    unsigned            i;

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
    pos = STORAGE_INDEXPAGE_HEADER_SIZE;

    i = 0;
    FOREACH (it, indexTree)
    {
        klen = it->key.GetLength();
        
        pos += sizeof(uint64_t);            // offset
        pos += sizeof(uint16_t);            // klen
        kpos = buffer.GetBuffer() + pos;        
        it->key = ReadBuffer(kpos, klen);
        it->keyBuffer.Reset();
        pos += klen;
        
        if (i == indexTree.GetCount() / 2)
            midpoint = it->key;
        
        i += 1;
    }
}

bool StorageIndexPage::Read(Buffer& buffer_)
{
    uint16_t                klen;
    uint32_t                size, checksum, compChecksum, numKeys, i;
    uint64_t                offset;
    ReadBuffer              dataPart, parse, key;
    StorageIndexRecord*     it;
    
    ASSERT(indexTree.GetCount() == 0);
    
    buffer.Write(buffer_);
    parse.Wrap(buffer);
    
    // size
    parse.ReadLittle32(size);
    if (size < STORAGE_INDEXPAGE_HEADER_SIZE)
        goto Fail;
    if (buffer.GetLength() != size)
        goto Fail;
    parse.Advance(4);

    // checksum
    if (false)  // TODO: make it switchable
    {
        parse.ReadLittle32(checksum);
        dataPart.Wrap(buffer.GetBuffer() + 8, buffer.GetLength() - 8);
        compChecksum = dataPart.GetChecksum();
        if (compChecksum != checksum)
            goto Fail;
    }
    parse.Advance(4);
    
    // numkeys
    parse.ReadLittle32(numKeys);
    parse.Advance(4);

    // keys
    for (i = 0; i < numKeys; i++)
    {
        // offset
        if (!parse.ReadLittle64(offset))
            goto Fail;
        parse.Advance(sizeof(offset));

        // klen
        if (!parse.ReadLittle16(klen))
            goto Fail;
        parse.Advance(sizeof(klen));
        
        // key
        if (parse.GetLength() < klen)
            goto Fail;
        key.Wrap(parse.GetBuffer(), klen);
        parse.Advance(klen);
        
        it = new StorageIndexRecord;
        it->key = key;
        it->index = i;
        it->offset = offset;
        indexTree.Insert<const ReadBuffer&>(it);
        
        if (i == numKeys / 2)
            midpoint = key;
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
