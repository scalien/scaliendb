#include "StorageChunkSerializer.h"
#include "PointerGuard.h"

StorageFile* StorageChunkWriter::Serialize(StorageChunk* chunk)
{
    PointerGuard<StorageFile> file(new StorageFile);
    
    if (chunk->UseBloomFilter())
    {
        P(file)->bloomPage.Clear();
        P(file)->bloomPage.SetNumKeys(chunk->GetNumKeys());
    }

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

    if (!WriteHeaderPage(P(file)))
        return NULL;

    return file.Release();
}

bool StorageChunkWriter::WriteHeaderPage(StorageChunk* chunk, StorageFile* file)
{
    file->headerPage.SetChunkID(chunk->GetChunkID());
    file->headerPage.SetLogSegmentID(chunk->GetLogSegmentID());
    file->headerPage.SetNumKeys(chunk->GetNumKeys());

    file->headerPage.SetVersion(STORAGE_HEADER_PAGE_VERSION);
    file->headerPage.SetUseBloomFilter(chunk->UseBloomFilter());
    file->headerPage.SetIndexPageOffset(indexPageOffset);
    file->headerPage.SetIndexPageSize(indexPageSize);
    file->headerPage.SetBloomPageOffset(bloomPageOffset);
    file->headerPage.SetBloomPageSize(bloomPageSize);
}

bool StorageChunkWriter::WriteDataPages(StorageChunk* chunk, StorageFile* file)
{
    StorageKeyValue*    it;
    StorageDataPage*    dataPage;

    dataPage = new StorageDataPage;
    dataPage.SetOffset(offset);
    FOREACH(it, chunk->keyValues)
    {
        if (chunk->UseBloomFilter())
            file->bloomPage.Add(it);

        if (dataPage->GetNumKeys() == 0)
        {
            dataPage->Append(it);
            file->indexPage.Append(it, offset);
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
    }
    
    return true;
}

bool StorageChunkWriter::WriteIndexPage(StorageFile* file)
{
    file->indexPage.SetOffset(offset);
    offset += indexPage.GetPageSize();
    return true;
}

bool StorageChunkWriter::WriteBloomPage(StorageFile*)
{
    file->bloomPage.SetOffset(offset);
    return true;
}
