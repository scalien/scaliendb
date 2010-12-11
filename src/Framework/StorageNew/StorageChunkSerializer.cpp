#include "StorageChunkSerializer.h"
#include "StorageMemoChunk.h"
#include "StorageFileChunk.h"
#include "PointerGuard.h"

StorageFileChunk* StorageChunkSerializer::Serialize(StorageMemoChunk* memoChunk_)
{
    PointerGuard<StorageFileChunk> fileGuard(new StorageFileChunk);
    
    memoChunk = memoChunk_;
    fileChunk = P(fileGuard);
    
    if (memoChunk->UseBloomFilter())
        fileChunk->bloomPage.SetNumKeys(memoChunk->keyValues.GetCount());

    offset = STORAGE_HEADER_PAGE_SIZE;
    
    if (!WriteDataPages())
        return NULL;

    if (!WriteIndexPage())
        return NULL;

    if (memoChunk->UseBloomFilter())
    {
        if (!WriteBloomPage())
            return NULL;
    }

    if (!WriteHeaderPage())
        return NULL;

    fileChunk->fileSize = offset;
    fileChunk->written = false;

    return fileGuard.Release();
}

bool StorageChunkSerializer::WriteHeaderPage()
{
    fileChunk->headerPage.SetOffset(0);
    
    fileChunk->headerPage.SetChunkID(memoChunk->GetChunkID());
    fileChunk->headerPage.SetLogSegmentID(memoChunk->GetLogSegmentID());
    fileChunk->headerPage.SetLogCommandID(memoChunk->GetLogCommandID());
    fileChunk->headerPage.SetNumKeys(memoChunk->keyValues.GetCount());

    fileChunk->headerPage.SetUseBloomFilter(memoChunk->UseBloomFilter());
    fileChunk->headerPage.SetIndexPageOffset(fileChunk->indexPage.GetOffset());
    fileChunk->headerPage.SetIndexPageSize(fileChunk->indexPage.GetSize());
    fileChunk->headerPage.SetBloomPageOffset(fileChunk->bloomPage.GetOffset());
    fileChunk->headerPage.SetBloomPageSize(fileChunk->bloomPage.GetSize());
    
    return true;
}

bool StorageChunkSerializer::WriteDataPages()
{
    StorageMemoKeyValue*    it;
    StorageDataPage*        dataPage;
    unsigned                dataPageIndex;
    
    dataPageIndex = 0;

    dataPage = new StorageDataPage;
    dataPage->SetOffset(offset);
    FOREACH(it, memoChunk->keyValues)
    {
        if (memoChunk->UseBloomFilter())
            fileChunk->bloomPage.Add(it->GetKey());

        if (dataPage->GetNumKeys() == 0)
        {
            dataPage->Append(it);
            fileChunk->indexPage.Append(it->GetKey(), dataPageIndex, offset);
        }
        else
        {
            if (dataPage->GetLength() + dataPage->GetIncrement(it) <= STORAGE_DEFAULT_DATA_PAGE_SIZE)
            {
                dataPage->Append(it);
            }
            else
            {
                dataPage->Finalize();
                fileChunk->AppendDataPage(dataPage);
                offset += dataPage->GetSize();
                dataPageIndex++;
                dataPage = new StorageDataPage;
                dataPage->SetOffset(offset);
            }
        }
    }

    // append last datapage
    if (dataPage->GetNumKeys() > 0)
    {
        dataPage->Finalize();
        fileChunk->AppendDataPage(dataPage);
        offset += dataPage->GetSize();
        dataPageIndex++;
    }
    
    return true;
}

bool StorageChunkSerializer::WriteIndexPage()
{
    fileChunk->indexPage.SetOffset(offset);
    offset += fileChunk->indexPage.GetSize();
    return true;
}

bool StorageChunkSerializer::WriteBloomPage()
{
    fileChunk->bloomPage.SetOffset(offset);
    offset += fileChunk->bloomPage.GetSize();
    return true;
}
