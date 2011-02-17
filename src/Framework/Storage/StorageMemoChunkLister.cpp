#include "StorageMemoChunkLister.h"

static inline int KeyCmp(const ReadBuffer& a, const ReadBuffer& b)
{
    return ReadBuffer::Cmp(a, b);
}

static inline const ReadBuffer Key(StorageMemoKeyValue* kv)
{
    return kv->GetKey();
}

// initialize dummy data page for storing key-values
StorageMemoChunkLister::StorageMemoChunkLister() : dataPage(NULL, 0)
{
}

void StorageMemoChunkLister::Load(StorageMemoChunk* chunk, ReadBuffer& startKey, 
 unsigned count, unsigned offset)
{
    StorageMemoKeyValue*    kv;
    int                     cmpres;
    unsigned                num;

    num = 0;
    
    kv = chunk->keyValues.Locate(startKey, cmpres);
    if (kv != NULL && cmpres > 0)
        kv = chunk->keyValues.Next(kv);

    while (kv != NULL)
    {
        dataPage.Append(kv);
        if (kv->GetType() == STORAGE_KEYVALUE_TYPE_SET)
        {
            num++;
            if (count != 0 && num == count + offset)
                break;
        }
        kv = chunk->keyValues.Next(kv);
    }
    
    dataPage.Finalize();
}

StorageFileKeyValue* StorageMemoChunkLister::First(ReadBuffer& firstKey)
{
    StorageFileKeyValue*    kv;
    
    kv = dataPage.First();
    while (kv != NULL && ReadBuffer::Cmp(kv->GetKey(), firstKey) < 0)
        kv = dataPage.Next(kv);
            
    return kv;
}

StorageFileKeyValue* StorageMemoChunkLister::Next(StorageFileKeyValue* kv)
{
    return dataPage.Next(kv);
}

uint64_t StorageMemoChunkLister::GetNumKeys()
{
    return dataPage.GetNumKeys();
}
