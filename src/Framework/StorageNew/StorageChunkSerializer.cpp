#include "StorageChunkSerializer.h"
#include "StorageChunk.h"
#include "StorageChunkFile.h"
#include "PointerGuard.h"

StorageChunkFile* StorageChunkSerializer::Serialize(StorageChunk* chunk)
{
    PointerGuard<StorageChunkFile> file(new StorageChunkFile);
    
    if (chunk->UseBloomFilter())
        P(file)->bloomPage.SetNumKeys(chunk->GetNumKeys());

    offset = STORAGE_HEADER_PAGE_SIZE;
    
    if (!WriteDataPages(chunk, P(file)))
        return NULL;

    if (!WriteIndexPage(P(file)))
        return NULL;

    if (chunk->UseBloomFilter())
    {
        if (!WriteBloomPage(P(file)))
            return NULL;
    }

    if (!WriteHeaderPage(chunk, P(file)))
        return NULL;

    return file.Release();
}

bool StorageChunkSerializer::WriteHeaderPage(StorageChunk* chunk, StorageChunkFile* file)
{
    file->headerPage.SetOffset(0);
    
    file->headerPage.SetChunkID(chunk->GetChunkID());
    file->headerPage.SetLogSegmentID(chunk->GetLogSegmentID());
    file->headerPage.SetLogCommandID(chunk->GetLogCommandID());
    file->headerPage.SetNumKeys(chunk->GetNumKeys());

    file->headerPage.SetUseBloomFilter(chunk->UseBloomFilter());
    file->headerPage.SetIndexPageOffset(file->indexPage.GetOffset());
    file->headerPage.SetIndexPageSize(file->indexPage.GetSize());
    file->headerPage.SetBloomPageOffset(file->bloomPage.GetOffset());
    file->headerPage.SetBloomPageSize(file->bloomPage.GetSize());
    
    return true;
}

bool StorageChunkSerializer::WriteDataPages(StorageChunk* chunk, StorageChunkFile* file)
{
    StorageKeyValue*    it;
    StorageDataPage*    dataPage;
    unsigned            dataPageIndex;
    
    dataPageIndex = 0;

    dataPage = new StorageDataPage;
    dataPage.SetOffset(offset);
    FOREACH(it, chunk->keyValues)
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
            if (dataPage->GetLength() + dataPage->GetSizeIncrement(it) <= STORAGE_DEFAULT_DATA_PAGE_SIZE)
            {
                dataPage->Append(it);
            }
            else
            {
                dataPage.Finalize();
                file->AppendDataPage(dataPage);
                offset += dataPage.GetPageSize();
                dataPageIndex++;
                dataPage = new StorageDataPage;
                dataPage.SetOffset(offset);
            }
        }
    }

    // append last datapage
    if (dataPage.GetNumKeys() > 0)
    {
        dataPage.Finalize();
        file->AppendDataPage(dataPage);
        offset += dataPage.GetPageSize();
        dataPageIndex++;
    }
    
    return true;
}

bool StorageChunkSerializer::WriteIndexPage(StorageChunkFile* file)
{
    file->indexPage.SetOffset(offset);
    offset += indexPage.GetSize();
    return true;
}

bool StorageChunkSerializer::WriteBloomPage(StorageChunkFile* file)
{
    file->bloomPage.SetOffset(offset);
    return true;
}
