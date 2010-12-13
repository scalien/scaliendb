#include "StorageDataPage.h"

static int KeyCmp(const ReadBuffer a, const ReadBuffer b)
{
    return ReadBuffer::Cmp(a, b);
}

static const ReadBuffer Key(StorageFileKeyValue* kv)
{
    return kv->GetKey();
}

StorageDataPage::StorageDataPage()
{
    size = 0;

    buffer.Allocate(STORAGE_DEFAULT_DATA_PAGE_GRAN);
    buffer.Zero();

    buffer.AppendLittle32(0); // dummy for size
    buffer.AppendLittle32(0); // dummy for checksum
    buffer.AppendLittle32(0); // dummy for numKeys
}

uint32_t StorageDataPage::GetSize()
{
    return size;
}

StorageKeyValue* StorageDataPage::Get(ReadBuffer& key)
{
    StorageFileKeyValue* it;

    int cmpres;
    
    if (keyValues.GetCount() == 0)
        return NULL;
        
    it = keyValues.Locate<ReadBuffer&>(key, cmpres);
    if (cmpres != 0)
        return NULL;

    return it;
}

uint32_t StorageDataPage::GetNumKeys()
{
    return keyValues.GetCount();
}

uint32_t StorageDataPage::GetLength()
{
    return buffer.GetLength();
}

uint32_t StorageDataPage::GetIncrement(StorageMemoKeyValue* kv)
{
    if (kv->GetType() == STORAGE_KEYVALUE_TYPE_SET)
        return (1 + 2 + kv->GetKey().GetLength() + 4 + kv->GetValue().GetLength());
    else if (kv->GetType() == STORAGE_KEYVALUE_TYPE_DELETE)
        return (1 + 2 + kv->GetKey().GetLength());
    else
        ASSERT_FAIL();

    return 0;
}

void StorageDataPage::Append(StorageMemoKeyValue* kv)
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
        fkv->Delete(ReadBuffer(kv->GetValue()));
    
    keyValues.Insert(fkv);
}

void StorageDataPage::Finalize()
{
    uint32_t                div, mod, numKeys, checksum, length, klen, vlen, pos;
    char                    *kpos, *vpos;
    ReadBuffer              dataPart;
    StorageFileKeyValue*    it;
    
    numKeys = keyValues.GetCount();
    length = buffer.GetLength();

    div = length / STORAGE_DEFAULT_DATA_PAGE_GRAN;
    mod = length % STORAGE_DEFAULT_DATA_PAGE_GRAN;
    size = div * STORAGE_DEFAULT_DATA_PAGE_GRAN;
    if (mod > 0)
        size += STORAGE_DEFAULT_DATA_PAGE_GRAN;

    buffer.Allocate(size);
    buffer.ZeroRest();

    // compute checksum
    dataPart.SetBuffer(buffer.GetBuffer() + 8);
    dataPart.SetLength(size - 8);
    checksum = dataPart.GetChecksum();

    buffer.SetLength(0);
    buffer.AppendLittle32(size);
    buffer.AppendLittle32(checksum);
    buffer.AppendLittle32(numKeys);

    buffer.SetLength(size);
    
    // set ReadBuffers in tree
    pos = 12;
    FOREACH(it, keyValues)
    {
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

void StorageDataPage::Write(Buffer& writeBuffer)
{
    writeBuffer.Write(buffer);
}
