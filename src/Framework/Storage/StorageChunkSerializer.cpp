#include "StorageChunkSerializer.h"
#include "StorageMemoChunk.h"
#include "StorageFileChunk.h"
#include "PointerGuard.h"

bool StorageChunkSerializer::Serialize(StorageMemoChunk* memoChunk_)
{
    PointerGuard<StorageFileChunk> fileGuard(new StorageFileChunk);
    
    memoChunk = memoChunk_;
    fileChunk = P(fileGuard);
    
    fileChunk->indexPage = new StorageIndexPage(fileChunk);
    
    if (memoChunk->UseBloomFilter())
    {
        fileChunk->bloomPage = new StorageBloomPage(fileChunk);
        fileChunk->bloomPage->SetNumKeys(memoChunk->keyValues.GetCount());
    }
    
    offset = STORAGE_HEADER_PAGE_SIZE;
    
    if (!WriteDataPages())
        return false;

    if (!WriteIndexPage())
        return false;

    if (memoChunk->UseBloomFilter())
    {
        if (!WriteBloomPage())
            return false;
    }

    if (!WriteHeaderPage())
        return false;

    fileChunk->minLogSegmentID = memoChunk->minLogSegmentID;
    fileChunk->fileSize = offset;
    fileChunk->written = false;

    memoChunk->fileChunk = fileGuard.Release();
    memoChunk->serialized = true;
    return true;
}

bool StorageChunkSerializer::WriteHeaderPage()
{
    fileChunk->headerPage.SetOffset(0);
    
    fileChunk->headerPage.SetChunkID(memoChunk->GetChunkID());
    fileChunk->headerPage.SetLogSegmentID(memoChunk->GetMaxLogSegmentID());
    fileChunk->headerPage.SetLogCommandID(memoChunk->GetMaxLogCommandID());
    fileChunk->headerPage.SetNumKeys(memoChunk->keyValues.GetCount());

    fileChunk->headerPage.SetUseBloomFilter(memoChunk->UseBloomFilter());
    fileChunk->headerPage.SetIndexPageOffset(fileChunk->indexPage->GetOffset());
    fileChunk->headerPage.SetIndexPageSize(fileChunk->indexPage->GetSize());
    if (memoChunk->UseBloomFilter())
    {
        fileChunk->headerPage.SetBloomPageOffset(fileChunk->bloomPage->GetOffset());
        fileChunk->headerPage.SetBloomPageSize(fileChunk->bloomPage->GetSize());
    }

    return true;
}

bool StorageChunkSerializer::WriteDataPages()
{
    StorageMemoKeyValue*    it;
    StorageDataPage*        dataPage;
    unsigned                dataPageIndex;

    dataPageIndex = 0;

    dataPage = new StorageDataPage(fileChunk, dataPageIndex);
    dataPage->SetOffset(offset);
    FOREACH (it, memoChunk->keyValues)
    {
        if (memoChunk->UseBloomFilter())
            fileChunk->bloomPage->Add(it->GetKey());

        if (dataPage->GetNumKeys() == 0)
        {
            dataPage->Append(it);
            fileChunk->indexPage->Append(it->GetKey(), dataPageIndex, offset);
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
                dataPage = new StorageDataPage(fileChunk, dataPageIndex);
                dataPage->SetOffset(offset);
                dataPage->Append(it);
                fileChunk->indexPage->Append(it->GetKey(), dataPageIndex, offset);
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
    
    fileChunk->indexPage->Finalize();

    return true;
}

bool StorageChunkSerializer::WriteIndexPage()
{
    fileChunk->indexPage->SetOffset(offset);
    offset += fileChunk->indexPage->GetSize();
    return true;
}

bool StorageChunkSerializer::WriteBloomPage()
{
    fileChunk->bloomPage->SetOffset(offset);
    offset += fileChunk->bloomPage->GetSize();
    return true;
}
