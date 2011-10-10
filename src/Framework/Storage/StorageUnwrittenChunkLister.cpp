#include "StorageUnwrittenChunkLister.h"

// initialize dummy data page for storing key-values
StorageUnwrittenChunkLister::StorageUnwrittenChunkLister() : dataPage(NULL, 0)
{
}

// copy key-values from file chunk to a temporary data page, that will be used for listing
void StorageUnwrittenChunkLister::Init(StorageFileChunk& fileChunk, ReadBuffer& firstKey,
 unsigned count, bool forwardDirection_)
{
    StorageFileKeyValue*    kv;
    int                     cmpres;
    unsigned                num;
    uint32_t                index;
    uint64_t                offset;
    int                     ret;

    Log_Debug("Listing unwritten chunk");

    num = 0;
    index = 0;
    forwardDirection = forwardDirection_;
    
    if (forwardDirection && ReadBuffer::Cmp(firstKey, fileChunk.indexPage->GetFirstKey()) < 0)
    {
        index = 0;
        offset = fileChunk.indexPage->GetFirstDatapageOffset();
    }
    else if (!forwardDirection && firstKey.GetLength() > 0 && ReadBuffer::Cmp(firstKey, fileChunk.indexPage->GetFirstKey()) < 0)
    {
        // firstKey is set and smaller that the first key in this chunk
        dataPage.Finalize();
        return;
    }
    else if (!forwardDirection && firstKey.GetLength() > 0 && ReadBuffer::Cmp(firstKey, fileChunk.indexPage->GetLastKey()) > 0)
    {
        index = fileChunk.numDataPages - 1;
        offset = fileChunk.indexPage->GetLastDatapageOffset();
    }
    else if (!forwardDirection && firstKey.GetLength() == 0)
    {
        index = fileChunk.numDataPages - 1;
        offset = fileChunk.indexPage->GetLastDatapageOffset();
    }
    else
    {
        ret = fileChunk.indexPage->Locate(firstKey, index, offset);
        ASSERT(ret);
    }

    kv = fileChunk.dataPages[index]->LocateKeyValue(firstKey, cmpres);
    if (kv != NULL)
    {
        if (forwardDirection && cmpres > 0)
            kv = NextChunkKeyValue(fileChunk, index, kv);
        else if (!forwardDirection && cmpres < 0)
            kv = PrevChunkKeyValue(fileChunk, index, kv);
    }

    while (kv != NULL)
    {
        dataPage.Append(kv);
        if (kv->GetType() == STORAGE_KEYVALUE_TYPE_SET)
        {
            num++;
            if (count != 0 && num == count)
                break;
        }
        if (forwardDirection)
            kv = NextChunkKeyValue(fileChunk, index, kv);
        else
            kv = PrevChunkKeyValue(fileChunk, index, kv);
    }
    
    dataPage.Finalize();
}

void StorageUnwrittenChunkLister::Load()
{
    // do nothing
}

void StorageUnwrittenChunkLister::SetDirection(bool forwardDirection_)
{
    ASSERT(forwardDirection == forwardDirection_);
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

StorageDataPage* StorageUnwrittenChunkLister::GetDataPage()
{
    return &dataPage;
}

StorageFileKeyValue* StorageUnwrittenChunkLister::NextChunkKeyValue(StorageFileChunk& fileChunk,
 uint32_t& index, StorageFileKeyValue* kv)
{
    StorageFileKeyValue*    next;
    
    next = fileChunk.dataPages[index]->Next(kv);
    
    if (next != NULL)
        return next;
    
    index++;
    if (index >= fileChunk.numDataPages)
        return NULL;

    return fileChunk.dataPages[index]->First();
}

StorageFileKeyValue* StorageUnwrittenChunkLister::PrevChunkKeyValue(StorageFileChunk& fileChunk,
 uint32_t& index, StorageFileKeyValue* kv)
{
    StorageFileKeyValue*    next;
    
    next = fileChunk.dataPages[index]->Prev(kv);
    
    if (next != NULL)
        return next;
    
    if (index == 0)
        return NULL;
    index--;

    return fileChunk.dataPages[index]->Last();
}
