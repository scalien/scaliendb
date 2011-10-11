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

void StorageMemoChunkLister::Init(
 StorageMemoChunk* chunk, ReadBuffer& firstKey, 
 unsigned count, bool keysOnly, bool forwardDirection)
{
    StorageMemoKeyValue*    kv;
    unsigned                num;

    kv = GetFirstKey(chunk, firstKey, forwardDirection);

    num = 0;
    while (kv != NULL)
    {
        dataPage.Append(kv, keysOnly);
        if (kv->GetType() == STORAGE_KEYVALUE_TYPE_SET)
        {
            num++;
            if (count != 0 && num == count)
                break;
        }
        if (forwardDirection)
            kv = chunk->keyValues.Next(kv);
        else
            kv = chunk->keyValues.Prev(kv);
    }
    
    dataPage.Finalize();
}

void StorageMemoChunkLister::Load()
{
    // do nothing
}

void StorageMemoChunkLister::SetDirection(bool forwardDirection_)
{
    forwardDirection = forwardDirection_;
}

StorageFileKeyValue* StorageMemoChunkLister::First(ReadBuffer& firstKey)
{
    UNUSED(firstKey);

    // use dataPage.First()
    // both in case of forward and backward iteration
    // because dataPage is a linear & unsorted store that
    // returns items in order of insertion
    // and we insert in order of iteration

    return dataPage.First();    
}

StorageFileKeyValue* StorageMemoChunkLister::Next(StorageFileKeyValue* kv)
{
    // use dataPage.Next()
    // both in case of forward and backward iteration
    // because dataPage is a linear & unsorted store that
    // returns items in order of insertion
    // and we insert in order of iteration

    return dataPage.Next(kv);
}

uint64_t StorageMemoChunkLister::GetNumKeys()
{
    return dataPage.GetNumKeys();
}

StorageDataPage* StorageMemoChunkLister::GetDataPage()
{
    return &dataPage;
}

StorageMemoKeyValue* StorageMemoChunkLister::GetFirstKey(
 StorageMemoChunk* chunk, ReadBuffer& startKey, bool forwardDirection)
{
    StorageMemoKeyValue*    kv;
    int                     cmpres;

    if (!forwardDirection && startKey.GetLength() == 0)
        return chunk->keyValues.Last();

    kv = chunk->keyValues.Locate(startKey, cmpres);

    if (forwardDirection)
    {
        // forward iteration
        if (kv != NULL && cmpres > 0)
            kv = chunk->keyValues.Next(kv);
    }
    else
    {
        // backward iteration
        if (kv != NULL && cmpres < 0)
            kv = chunk->keyValues.Prev(kv);
    }

    return kv;
}
