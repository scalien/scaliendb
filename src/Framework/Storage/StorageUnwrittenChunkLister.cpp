#include "StorageUnwrittenChunkLister.h"

// initialize dummy data page for storing key-values
StorageUnwrittenChunkLister::StorageUnwrittenChunkLister() : dataPage(NULL, 0)
{
}

// copy key-values from file chunk to a temporary data page, that will be used for listing
void StorageUnwrittenChunkLister::Init(StorageFileChunk* chunk, ReadBuffer& startKey,
 unsigned count, unsigned offset)
{
    StorageFileKeyValue*    kv;
    int                     cmpres;
    unsigned                num;
    uint32_t                index;
    uint32_t                pageOffset;
    int                     ret;

    num = 0;
    index = 0;
    
    if (ReadBuffer::Cmp(startKey, chunk->indexPage->GetFirstKey()) >= 0)
    {
        ret = chunk->indexPage->Locate(startKey, index, pageOffset);
        ASSERT(ret);
    }

    kv = chunk->dataPages[index]->LocateKeyValue(startKey, cmpres);
    if (kv != NULL && cmpres > 0)
        kv = NextChunkKeyValue(chunk, index, kv);

    while (kv != NULL)
    {
        dataPage.Append(kv);
        if (kv->GetType() == STORAGE_KEYVALUE_TYPE_SET)
        {
            num++;
            if (count != 0 && num == count + offset)
                break;
        }
        kv = NextChunkKeyValue(chunk, index, kv);
    }
    
    dataPage.Finalize();
}

void StorageUnwrittenChunkLister::Load()
{
    // do nothing
}

StorageFileKeyValue* StorageUnwrittenChunkLister::First(ReadBuffer& firstKey)
{
    StorageFileKeyValue*    kv;
    
    kv = dataPage.First();
    while (kv != NULL && ReadBuffer::Cmp(kv->GetKey(), firstKey) < 0)
        kv = dataPage.Next(kv);
            
    return kv;
}

StorageFileKeyValue* StorageUnwrittenChunkLister::Next(StorageFileKeyValue* kv)
{
    return dataPage.Next(kv);
}
    
uint64_t StorageUnwrittenChunkLister::GetNumKeys()
{
    return dataPage.GetNumKeys();
}

StorageFileKeyValue* StorageUnwrittenChunkLister::NextChunkKeyValue(StorageFileChunk* chunk,
 uint32_t& index, StorageFileKeyValue* kv)
{
    StorageFileKeyValue*    next;
    
    next = chunk->dataPages[index]->Next(kv);
    
    if (next != NULL)
        return next;
    
    index++;
    if (index >= chunk->numDataPages)
        return NULL;

    return chunk->dataPages[index]->First();
}
