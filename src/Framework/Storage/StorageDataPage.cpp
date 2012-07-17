#include "StorageDataPage.h"
#include "StorageFileChunk.h"
#include "System/Containers/InList.h"
#include "System/Threading/Mutex.h"

#define STORAGE_DATAPAGE_HEADER_SIZE        16

StorageDataPage::StorageDataPage()
{
    prev = next = this;
}

StorageDataPage::StorageDataPage(StorageFileChunk* owner_, uint32_t index_, unsigned bufferSize)
{
    prev = next = this;
    Init(owner_, index_, bufferSize);
}

StorageDataPage::~StorageDataPage()
{
}

void StorageDataPage::Init(StorageFileChunk* owner_, uint32_t index_, unsigned bufferSize)
{
    size = 0;
    compressedSize = 0;

    keysBuffer.SetLength(0);
    valuesBuffer.SetLength(0);

    buffer.Allocate(bufferSize);
    buffer.Zero();
    buffer.SetLength(0);

    buffer.AppendLittle32(0); // dummy for size
    buffer.AppendLittle32(0); // dummy for checksum
    buffer.AppendLittle32(0); // dummy for keysSize
    buffer.AppendLittle32(0); // dummy for numKeys
    
    storageFileKeyValueBuffer.SetLength(0);

    owner = owner_;
    index = index_;
}

void StorageDataPage::SetOwner(StorageFileChunk* owner_)
{
    ASSERT(owner == NULL);
    owner = owner_;
}

uint32_t StorageDataPage::GetSize()
{
    return size;
}

uint32_t StorageDataPage::GetMemorySize()
{
    return buffer.GetSize() + keysBuffer.GetSize() + 
        valuesBuffer.GetSize() + storageFileKeyValueBuffer.GetSize();
}

uint32_t StorageDataPage::GetCompressedSize()
{
    return compressedSize;
}

uint32_t StorageDataPage::GetPageBufferSize()
{
    return buffer.GetLength() + keysBuffer.GetLength() + valuesBuffer.GetLength();
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
    return storageFileKeyValueBuffer.GetLength() / sizeof(StorageFileKeyValue);
}

uint32_t StorageDataPage::GetLength()
{
    return STORAGE_DATAPAGE_HEADER_SIZE + keysBuffer.GetLength() + valuesBuffer.GetLength();
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

uint32_t StorageDataPage::GetIndex()
{
    return index;
}

void StorageDataPage::Append(StorageKeyValue* kv, bool keysOnly)
{
    StorageFileKeyValue fkv;

    ASSERT(kv->GetKey().GetLength() > 0);

    keysBuffer.Append(kv->GetType());                               // 1 byte(s)
    keysBuffer.AppendLittle16(kv->GetKey().GetLength());            // 2 byte(s)
    keysBuffer.Append(kv->GetKey());
    if (kv->GetType() == STORAGE_KEYVALUE_TYPE_SET && keysOnly == false)
    {
        valuesBuffer.AppendLittle32(kv->GetValue().GetLength());    // 4 bytes(s)
        valuesBuffer.Append(kv->GetValue());
    }

    if (kv->GetType() == STORAGE_KEYVALUE_TYPE_SET)
    {
        if (keysOnly)
            fkv.Set(ReadBuffer(kv->GetKey()), ReadBuffer());
        else
            fkv.Set(ReadBuffer(kv->GetKey()), ReadBuffer(kv->GetValue()));
    }
    else
        fkv.Delete(ReadBuffer(kv->GetKey()));
    
    AppendKeyValue(fkv);
}

void StorageDataPage::Finalize()
{
    uint32_t                div, mod, numKeys, checksum, length, klen, vlen, kit, vit;
    char                    *kpos, *vpos;
    ReadBuffer              dataPart;
    StorageFileKeyValue*    it;
    
    numKeys = GetNumKeys();
    buffer.Append(keysBuffer);
    buffer.Append(valuesBuffer);
    length = buffer.GetLength();

    div = length / STORAGE_DEFAULT_PAGE_GRAN;
    mod = length % STORAGE_DEFAULT_PAGE_GRAN;
    size = div * STORAGE_DEFAULT_PAGE_GRAN;
    if (mod > 0)
        size += STORAGE_DEFAULT_PAGE_GRAN;

    buffer.Allocate(size);
    buffer.ZeroRest();

    // write keysSize
    buffer.SetLength(8);
    buffer.AppendLittle32(keysBuffer.GetLength());

    // write numKeys
    buffer.SetLength(12);
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
    kit = STORAGE_DATAPAGE_HEADER_SIZE;
    vit = STORAGE_DATAPAGE_HEADER_SIZE + keysBuffer.GetLength();
    for (unsigned i = 0; i < numKeys; i++)
    {
        it = GetIndexedKeyValue(i);
        ASSERT(it->GetKey().GetLength() > 0);
        
        kit += 1;                                                // type
        if (it->GetType() == STORAGE_KEYVALUE_TYPE_SET)
        {
            klen = it->GetKey().GetLength();
            vlen = it->GetValue().GetLength();
            
            kit += 2;                                            // keylen
            kpos = buffer.GetBuffer() + kit;
            kit += klen;
            vit += 4;                                            // vlen
            vpos = buffer.GetBuffer() + vit;
            vit += vlen;
            
            it->Set(ReadBuffer(kpos, klen), ReadBuffer(vpos, vlen));
        }
        else
        {
            klen = it->GetKey().GetLength();
            
            kit += 2;                                            // keylen
            kpos = buffer.GetBuffer() + kit;
            kit += klen;
            
            it->Delete(ReadBuffer(kpos, klen));
        }
    }
    
#ifdef STORAGE_DATAPAGE_COMPRESSION
    Buffer                  compressed;
    Compressor              compressor;
    
    compressor.Compress(buffer, compressed);
    buffer.SetLength(0);
    buffer.AppendLittle32(0);
    buffer.AppendLittle32(0);  // checksum
    buffer.AppendLittle32(keysBuffer.GetLength());
    buffer.AppendLittle32(buffer.GetLength());
    buffer.Append(compressed);
    
    length = buffer.GetLength();
    compressedSize = compressed.GetLength();
    buffer.SetLength(0);
    buffer.AppendLittle32(length);
    buffer.SetLength(length);
#else
    compressedSize = size;
#endif

    keysBuffer.Reset();
    valuesBuffer.Reset();
}

void StorageDataPage::Reset()
{
    storageFileKeyValueBuffer.Reset();
    
    keysBuffer.Reset();
    valuesBuffer.Reset();
    buffer.Reset();
    
    buffer.AppendLittle32(0); // dummy for size
    buffer.AppendLittle32(0); // dummy for checksum
    buffer.AppendLittle32(0); // dummy for keysLength
    buffer.AppendLittle32(0); // dummy for numKeys
}

StorageFileKeyValue* StorageDataPage::First()
{
    return GetIndexedKeyValue(0);
}

StorageFileKeyValue* StorageDataPage::Last()
{
    return GetIndexedKeyValue(GetNumKeys() - 1);
}

StorageFileKeyValue* StorageDataPage::Next(StorageFileKeyValue* it)
{
    unsigned index;

    index = ((char*) it - storageFileKeyValueBuffer.GetBuffer()) / sizeof(StorageFileKeyValue);
    return GetIndexedKeyValue(index + 1);
}

StorageFileKeyValue* StorageDataPage::Prev(StorageFileKeyValue* it)
{
    unsigned index;

    index = ((char*) it - storageFileKeyValueBuffer.GetBuffer()) / sizeof(StorageFileKeyValue);
    return GetIndexedKeyValue(index - 1);
}

StorageFileKeyValue* StorageDataPage::GetIndexedKeyValue(unsigned index)
{
    if (index >= (storageFileKeyValueBuffer.GetLength() / sizeof(StorageFileKeyValue)))
        return NULL;
    return (StorageFileKeyValue*) (storageFileKeyValueBuffer.GetBuffer() + index * sizeof(StorageFileKeyValue));
}

StorageFileKeyValue* StorageDataPage::LocateKeyValue(ReadBuffer& key, int& cmpres)
{
    StorageFileKeyValue*    kvIndex;
    unsigned                first;
    unsigned                last;
    unsigned                numKeys;
    unsigned                mid;

    cmpres = 0;
    numKeys = GetNumKeys();
    if (numKeys == 0)
        return NULL;
    
    kvIndex = (StorageFileKeyValue*) storageFileKeyValueBuffer.GetBuffer();
    
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
        cmpres = ReadBuffer::Cmp(key, kvIndex[mid].GetKey());
        if (cmpres == 0)
            return &kvIndex[mid];
        
        if (cmpres < 0)
        {
            if (mid == 0)
                return &kvIndex[mid];

            last = mid - 1;
        }
        else 
            first = mid + 1;
    }
    
    // not found
    return &kvIndex[mid];
}

bool StorageDataPage::Read(Buffer& buffer_, bool keysOnly)
{
    char                    type;
    uint16_t                klen;
    uint32_t                size, /*checksum, compChecksum,*/ numKeys, vlen, i, keysSize;
    ReadBuffer              dataPart, parse, kparse, vparse, key, value;
    StorageFileKeyValue     fkv;
    
#ifdef STORAGE_DATAPAGE_COMPRESSION    
    Compressor              compressor;
    uint32_t                uncompressedSize;

    Key - value seperation not implemented!

    parse.Wrap(buffer_);
    parse.ReadLittle32(compressedSize);
    parse.Advance(sizeof(uint32_t));
    parse.ReadLittle32(uncompressedSize);
    parse.Advance(sizeof(uint32_t));
    compressor.Uncompress(parse, buffer, uncompressedSize);
#else
    ASSERT(GetNumKeys() == 0);
    buffer.Write(buffer_);
#endif
    parse.Wrap(buffer);
    
    // size
    parse.ReadLittle32(size);
    if (size < 12)
        goto Fail;
    if (!keysOnly)
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

    // keysSize
    parse.ReadLittle32(keysSize);
    parse.Advance(4);
    
    // numkeys
    parse.ReadLittle32(numKeys);
    parse.Advance(4);

    // preallocate keyValue buffer
    storageFileKeyValueBuffer.Allocate(numKeys * sizeof(StorageFileKeyValue));

    // keys
    kparse = parse;
    if (!keysOnly)
    {
        vparse = parse;
        vparse.Advance(keysSize);
    }
    for (i = 0; i < numKeys; i++)
    {
        // type
        if (!kparse.ReadChar(type))
            goto Fail;
        if (type != STORAGE_KEYVALUE_TYPE_SET && type != STORAGE_KEYVALUE_TYPE_DELETE)
            goto Fail;
        kparse.Advance(1);

        // klen
        if (!kparse.ReadLittle16(klen))
            goto Fail;
        kparse.Advance(2);
        ASSERT(klen > 0);
        
        // key
        if (kparse.GetLength() < klen)
            goto Fail;
        key.Wrap(kparse.GetBuffer(), klen);
        kparse.Advance(klen);

        if (type == STORAGE_KEYVALUE_TYPE_SET)
        {
            if (!keysOnly)
            {
                // vlen
                if (!vparse.ReadLittle32(vlen))
                    goto Fail;
                vparse.Advance(4);
                
                // value
                if (vparse.GetLength() < vlen)
                    goto Fail;
                value.Wrap(vparse.GetBuffer(), vlen);
                vparse.Advance(vlen);
            }
            else
                value.Reset();

            fkv.Set(key, value);
            AppendKeyValue(fkv);
        }
        else
        {
            fkv.Delete(key);
            AppendKeyValue(fkv);
        }
    }

#ifdef STORAGE_DATAPAGE_COMPRESSION
    this->size = uncompressedSize;
#else    
    this->size = size;
    this->compressedSize = size;
#endif
    return true;
    
Fail:
    storageFileKeyValueBuffer.Reset();
    buffer.Reset();
    return false;
}

void StorageDataPage::Write(Buffer& buffer_)
{
    buffer_.Write(buffer);
}

unsigned StorageDataPage::Serialize(Buffer& buffer_)
{
    buffer_.Append(buffer);
    return buffer.GetLength();
}

void StorageDataPage::Unload()
{
    Reset();
    owner->OnDataPageEvicted(index);
}

void StorageDataPage::AppendKeyValue(StorageFileKeyValue& kv)
{
    ASSERT(kv.GetKey().GetLength() > 0);
    storageFileKeyValueBuffer.Append((const char*) &kv, sizeof(StorageFileKeyValue));
}
