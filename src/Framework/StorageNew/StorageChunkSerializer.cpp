#include "StorageChunkSerializer.h"
#include "StorageChunk.h"
#include "StorageChunkFile.h"
#include "PointerGuard.h"

StorageChunkFile* StorageChunkSerializer::Serialize(StorageChunk* chunk_)
{
    PointerGuard<StorageChunkFile> fileGuard(new StorageChunkFile);
    
    chunk = chunk_;
    file = P(fileGuard);
    
    if (chunk->UseBloomFilter())
        file->bloomPage.SetNumKeys(chunk->GetKeyValueTree().GetCount());

    offset = STORAGE_HEADER_PAGE_SIZE;
    
    if (!WriteDataPages())
        return NULL;

    if (!WriteIndexPage())
        return NULL;

    if (chunk->UseBloomFilter())
    {
        if (!WriteBloomPage())
            return NULL;
    }

    if (!WriteHeaderPage())
        return NULL;

    return fileGuard.Release();
}

bool StorageChunkSerializer::WriteHeaderPage()
{
    file->headerPage.SetOffset(0);
    
    file->headerPage.SetChunkID(chunk->GetChunkID());
    file->headerPage.SetLogSegmentID(chunk->GetLogSegmentID());
    file->headerPage.SetLogCommandID(chunk->GetLogCommandID());
    file->headerPage.SetNumKeys(chunk->GetKeyValueTree().GetCount());

    file->headerPage.SetUseBloomFilter(chunk->UseBloomFilter());
    file->headerPage.SetIndexPageOffset(file->indexPage.GetOffset());
    file->headerPage.SetIndexPageSize(file->indexPage.GetSize());
    file->headerPage.SetBloomPageOffset(file->bloomPage.GetOffset());
    file->headerPage.SetBloomPageSize(file->bloomPage.GetSize());
    
    return true;
}

bool StorageChunkSerializer::WriteDataPages()
{
    StorageKeyValue*    it;
    StorageDataPage*    dataPage;
    unsigned            dataPageIndex;
    
    dataPageIndex = 0;

    dataPage = new StorageDataPage;
    dataPage->SetOffset(offset);
    FOREACH(it, chunk->GetKeyValueTree())
    {
        if (chunk->UseBloomFilter())
            file->bloomPage.Add(it->GetKey());

        if (dataPage->GetNumKeys() == 0)
        {
            dataPage->Append(it);
            file->indexPage.Append(it->GetKey(), dataPageIndex, offset);
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
                file->AppendDataPage(dataPage);
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
        file->AppendDataPage(dataPage);
        offset += dataPage->GetSize();
        dataPageIndex++;
    }
    
    return true;
}

bool StorageChunkSerializer::WriteIndexPage()
{
    file->indexPage.SetOffset(offset);
    offset += file->indexPage.GetSize();
    return true;
}

bool StorageChunkSerializer::WriteBloomPage()
{
    file->bloomPage.SetOffset(offset);
    return true;
}
