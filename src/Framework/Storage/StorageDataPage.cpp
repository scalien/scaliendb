#include "StorageDataPage.h"
#include "StorageFileChunk.h"
#include "System/Compress/Compressor.h"

StorageDataPage::StorageDataPage(StorageFileChunk* owner_, uint32_t index_)
{
    size = 0;
    compressedSize = 0;

    buffer.Allocate(STORAGE_DEFAULT_PAGE_GRAN);
    buffer.Zero();

    buffer.AppendLittle32(0); // dummy for size
    buffer.AppendLittle32(0); // dummy for checksum
    buffer.AppendLittle32(0); // dummy for numKeys
    
    owner = owner_;
    index = index_;
}

StorageDataPage::~StorageDataPage()
{
    StorageFileKeyValue*    kv;

    for (unsigned i = 0; i < GetNumKeys(); i++)
    {
        kv = GetIndexedKeyValue(i);
        delete kv;
    }
}

uint32_t StorageDataPage::GetSize()
{
    return size;
}

uint32_t StorageDataPage::GetCompressedSize()
{
    return compressedSize;
}

StorageKeyValue* StorageDataPage::Get(ReadBuffer& key)
{
    StorageFileKeyValue* it;

    int cmpres;
    
    if (GetNumKeys() == 0)
        return NULL;
        
    it = LocateKeyValue(key, cmpres);
    if (cmpres != 0)
        return NULL;

    return it;
}

uint32_t StorageDataPage::GetNumKeys()
{
    return keyValueIndexBuffer.GetLength() / sizeof(StorageFileKeyValue*);
}

uint32_t StorageDataPage::GetLength()
{
    return buffer.GetLength();
}

uint32_t StorageDataPage::GetIncrement(StorageKeyValue* kv)
{
    if (kv->GetType() == STORAGE_KEYVALUE_TYPE_SET)
        return (1 + 2 + kv->GetKey().GetLength() + 4 + kv->GetValue().GetLength());
    else if (kv->GetType() == STORAGE_KEYVALUE_TYPE_DELETE)
        return (1 + 2 + kv->GetKey().GetLength());
    else
        ASSERT_FAIL();

    return 0;
}

void StorageDataPage::Append(StorageKeyValue* kv)
{
    StorageFileKeyValue*    fkv;
    
    buffer.Append(kv->GetType());                           // 1 byte(s)
    buffer.AppendLittle16(kv->GetKey().GetLength());        // 2 byte(s)
    buffer.Append(kv->GetKey());
    if (kv->GetType() == STORAGE_KEYVALUE_TYPE_SET)
    {
        buffer.AppendLittle32(kv->GetValue().GetLength());  // 4 bytes(s)
        buffer.Append(kv->GetValue());
    }

    fkv = new StorageFileKeyValue;
    if (kv->GetType() == STORAGE_KEYVALUE_TYPE_SET)
        fkv->Set(ReadBuffer(kv->GetKey()), ReadBuffer(kv->GetValue()));
    else
        fkv->Delete(ReadBuffer(kv->GetKey()));
    
    AppendKeyValue(fkv);
}

void StorageDataPage::Finalize()
{
    uint32_t                div, mod, numKeys, checksum, length, klen, vlen, pos;
    char                    *kpos, *vpos;
    ReadBuffer              dataPart;
    StorageFileKeyValue*    it;
    
    numKeys = GetNumKeys();
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
//    dataPart.Wrap(buffer.GetBuffer() + 8, size - 8);
//    checksum = dataPart.GetChecksum();
    checksum = 0;

    buffer.SetLength(0);
    buffer.AppendLittle32(size);
    buffer.AppendLittle32(checksum);

    buffer.SetLength(size);
    
    // set ReadBuffers in tree
    pos = 12;
    for (unsigned i = 0; i < numKeys; i++)
    {
        it = GetIndexedKeyValue(i);
        
        pos += 1;                                                // type
        if (it->GetType() == STORAGE_KEYVALUE_TYPE_SET)
        {
            klen = it->GetKey().GetLength();
            vlen = it->GetValue().GetLength();
            
            pos += 2;                                            // keylen
            kpos = buffer.GetBuffer() + pos;
            pos += klen;
            pos += 4;                                            // vlen
            vpos = buffer.GetBuffer() + pos;
            pos += vlen;
            
            it->Set(ReadBuffer(kpos, klen), ReadBuffer(vpos, vlen));
        }
        else
        {
            klen = it->GetKey().GetLength();
            
            pos += 2;                                            // keylen
            kpos = buffer.GetBuffer() + pos;
            pos += klen;
            
            it->Delete(ReadBuffer(kpos, klen));
        }
    }
}

void StorageDataPage::Reset()
{
    StorageFileKeyValue*    kv;

    for (unsigned i = 0; i < GetNumKeys(); i++)
    {
        kv = GetIndexedKeyValue(i);
        delete kv;
    }

    keyValueIndexBuffer.Reset();
    buffer.Reset();
    
    buffer.AppendLittle32(0); // dummy for size
    buffer.AppendLittle32(0); // dummy for checksum
    buffer.AppendLittle32(0); // dummy for numKeys
}

StorageFileKeyValue* StorageDataPage::First()
{
    return GetIndexedKeyValue(0);
}

StorageFileKeyValue* StorageDataPage::Next(StorageFileKeyValue* it)
{
    return GetIndexedKeyValue(it->GetNextIndex());
}

StorageFileKeyValue* StorageDataPage::GetIndexedKeyValue(unsigned index)
{
    if (index >= (keyValueIndexBuffer.GetLength() / sizeof(StorageFileKeyValue*)))
        return NULL;
    return ((StorageFileKeyValue**) keyValueIndexBuffer.GetBuffer())[index];
}

StorageFileKeyValue* StorageDataPage::LocateKeyValue(ReadBuffer& key, int& cmpres)
{
    StorageFileKeyValue**   kvIndex;
    unsigned                first;
    unsigned                last;
    unsigned                numKeys;
    unsigned                mid;

    cmpres = 0;
    numKeys = GetNumKeys();
    if (numKeys == 0)
        return NULL;
    
    kvIndex = (StorageFileKeyValue**) keyValueIndexBuffer.GetBuffer();
    
    if (key.GetLength() == 0)
    {
        cmpres = -1;
        return GetIndexedKeyValue(0);
    }
    
    first = 0;
    last = numKeys - 1;
    while (first <= last)
    {
        mid = first + ((last - first) / 2);
        cmpres = ReadBuffer::Cmp(key, kvIndex[mid]->GetKey());
        if (cmpres == 0)
            return kvIndex[mid];
        
        if (cmpres < 0)
        {
            if (mid == 0)
                return kvIndex[mid];

            last = mid - 1;
        }
        else 
            first = mid + 1;
    }
    
    // not found
    return kvIndex[mid];
}

bool StorageDataPage::Read(Buffer& buffer_)
{
    char                    type;
    uint16_t                klen;
    uint32_t                size, checksum, compChecksum, numKeys, vlen, i;
    ReadBuffer              dataPart, parse, key, value;
    StorageFileKeyValue*    fkv;
    
#ifdef STORAGE_DATAPAGE_COMPRESSION    
    Compressor              compressor;
    uint32_t                uncompressedLength;

    parse.Wrap(buffer_);
    parse.ReadLittle32(uncompressedLength);
    parse.Advance(sizeof(uint32_t));
    compressor.Uncompress(parse, buffer, uncompressedLength);
#else
    assert(GetNumKeys() == 0);    
    buffer.Write(buffer_);
#endif
    parse.Wrap(buffer);
    
    // size
    parse.ReadLittle32(size);
    if (size < 12)
        goto Fail;
    if (buffer.GetLength() != size)
        goto Fail;
    parse.Advance(4);

    // checksum
//    parse.ReadLittle32(checksum);
//    dataPart.Wrap(buffer.GetBuffer() + 8, buffer.GetLength() - 8);
//    compChecksum = dataPart.GetChecksum();
//    if (compChecksum != checksum)
//        goto Fail;
    parse.Advance(4);
    
    // numkeys
    parse.ReadLittle32(numKeys);
    parse.Advance(4);

    // keys
    for (i = 0; i < numKeys; i++)
    {
        // type
        if (!parse.ReadChar(type))
            goto Fail;
        if (type != STORAGE_KEYVALUE_TYPE_SET && type != STORAGE_KEYVALUE_TYPE_DELETE)
            goto Fail;
        parse.Advance(1);

        // klen
        if (!parse.ReadLittle16(klen))
            goto Fail;
        parse.Advance(2);
        
        // key
        if (parse.GetLength() < klen)
            goto Fail;
        key.Wrap(parse.GetBuffer(), klen);
        parse.Advance(klen);
        
        if (type == STORAGE_KEYVALUE_TYPE_SET)
        {
            // vlen
            if (!parse.ReadLittle32(vlen))
                goto Fail;
            parse.Advance(4);
            
            // value
            if (parse.GetLength() < vlen)
                goto Fail;
            value.Wrap(parse.GetBuffer(), vlen);
            parse.Advance(vlen);
            
            fkv = new StorageFileKeyValue;
            fkv->Set(key, value);
            AppendKeyValue(fkv);
        }
        else
        {
            fkv = new StorageFileKeyValue;
            fkv->Delete(key);
            AppendKeyValue(fkv);
        }
    }
    
    this->size = size;
    return true;
    
Fail:
    keyValueIndexBuffer.Reset();
    buffer.Reset();
    return false;
}

void StorageDataPage::Write(Buffer& buffer_)
{
#ifdef STORAGE_DATAPAGE_COMPRESSION
    Buffer                  compressed;
    Compressor              compressor;
    
    compressor.Compress(buffer, compressed);
    buffer_.SetLength(0);
    buffer_.AppendLittle32(buffer.GetLength());
    buffer_.Append(compressed);
    
    compressedSize = compressed.GetLength();
#else
    buffer_.Write(buffer);
#endif
}

void StorageDataPage::Unload()
{
    Reset();
    owner->OnDataPageEvicted(index);
}

void StorageDataPage::AppendKeyValue(StorageFileKeyValue* kv)
{
    keyValueIndexBuffer.Append((const char*) &kv, sizeof(StorageFileKeyValue*));
    kv->SetNextIndex(keyValueIndexBuffer.GetLength() / sizeof(StorageFileKeyValue*));
}
