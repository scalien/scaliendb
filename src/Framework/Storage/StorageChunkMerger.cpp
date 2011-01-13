#include "StorageChunkMerger.h"
#include "StorageKeyValue.h"
#include "System/FileSystem.h"

bool StorageChunkMerger::Merge(
 ReadBuffer filename1, ReadBuffer filename2, StorageFileChunk* mergeChunk_,  
 ReadBuffer firstKey, ReadBuffer lastKey)
{
    mergeChunk = mergeChunk_;
    
    // open readers
    reader1.Open(filename1);
    reader2.Open(filename2);

    // open writer
    if (fd.Open(mergeChunk->GetFilename().GetBuffer(), FS_CREATE | FS_READWRITE) == INVALID_FD)
        return false;

    offset = 0;
    numKeys = 0;

    mergeChunk->indexPage = new StorageIndexPage(mergeChunk);

    if (mergeChunk->UseBloomFilter())
    {
        mergeChunk->bloomPage = new StorageBloomPage(mergeChunk);
        mergeChunk->bloomPage->SetNumKeys(reader1.GetNumKeys() + reader2.GetNumKeys());
    }

    if (!WriteEmptyHeaderPage())
        return false;

    if (!WriteDataPages(firstKey, lastKey))
        return false;

    if (!WriteIndexPage())
        return false;

    if (mergeChunk->UseBloomFilter())
    {
        if (!WriteBloomPage())
            return false;
    }

    FS_Sync(fd.GetFD());
    
    FS_FileSeek(fd.GetFD(), 0, FS_SEEK_SET);
    offset = 0;

    if (!WriteHeaderPage())
        return false;

    FS_Sync(fd.GetFD());

    fd.Close();
    
    mergeChunk->fileSize = offset;
    mergeChunk->written = true;
    
//    Log_Message("n1 = %U   n2 = %U    n = %U", reader1.GetNumKeys(), reader2.GetNumKeys(), mergeChunk->headerPage.GetNumKeys());

    UnloadChunk();

    return true;
}

StorageFileKeyValue* StorageChunkMerger::Merge(StorageFileKeyValue* , StorageFileKeyValue* it2)
{
    if (it2->GetType() == STORAGE_KEYVALUE_TYPE_SET)
        return it2;
    else
        return NULL;
}

bool StorageChunkMerger::WriteBuffer()
{
    ssize_t     writeSize;

    writeSize = writeBuffer.GetLength();
    if (FS_FileWrite(fd.GetFD(), writeBuffer.GetBuffer(), writeSize) != writeSize)
        return false;
    
    offset += writeSize;

    return true;
}

void StorageChunkMerger::UnloadChunk()
{
    delete mergeChunk->indexPage;
    delete mergeChunk->bloomPage;
    mergeChunk->indexPage = NULL;
    mergeChunk->bloomPage = NULL;
}

bool StorageChunkMerger::WriteEmptyHeaderPage()
{
    uint32_t    pageSize;

    pageSize = mergeChunk->headerPage.GetSize();

    writeBuffer.Allocate(pageSize);
    writeBuffer.SetLength(pageSize);
    writeBuffer.Zero();

    if (!WriteBuffer())
        return false;

    return true;
}

bool StorageChunkMerger::WriteHeaderPage()
{
    mergeChunk->headerPage.SetOffset(0);
    
//    mergeChunk->headerPage.SetChunkID(memoChunk->GetChunkID());
    mergeChunk->headerPage.SetMinLogSegmentID(reader1.GetMinLogSegmentID());
    mergeChunk->headerPage.SetMaxLogSegmentID(reader2.GetMaxLogSegmentID());
    mergeChunk->headerPage.SetMaxLogCommandID(reader2.GetMaxLogCommandID());
    mergeChunk->headerPage.SetNumKeys(numKeys);

    mergeChunk->headerPage.SetIndexPageOffset(mergeChunk->indexPage->GetOffset());
    mergeChunk->headerPage.SetIndexPageSize(mergeChunk->indexPage->GetSize());
    if (mergeChunk->bloomPage)
    {
        mergeChunk->headerPage.SetBloomPageOffset(mergeChunk->bloomPage->GetOffset());
        mergeChunk->headerPage.SetBloomPageSize(mergeChunk->bloomPage->GetSize());
    }
    if (firstKey.GetLength() > 0)
    {
        mergeChunk->headerPage.SetFirstKey(ReadBuffer(firstKey));
        mergeChunk->headerPage.SetLastKey(ReadBuffer(lastKey));
        mergeChunk->headerPage.SetMidpoint(mergeChunk->indexPage->GetMidpoint());
    }

    writeBuffer.Clear();
    mergeChunk->headerPage.Write(writeBuffer);
    assert(writeBuffer.GetLength() == mergeChunk->headerPage.GetSize());

    if (!WriteBuffer())
        return false;

    return true;
}

bool StorageChunkMerger::WriteDataPages(ReadBuffer firstKey, ReadBuffer lastKey)
{
    bool                    advance1;
    bool                    advance2;
    bool                    skip;
    uint32_t                index;
    int                     cmp;
    StorageFileKeyValue*    it1;
    StorageFileKeyValue*    it2;
    StorageFileKeyValue*    it;
    StorageDataPage*        dataPage;
    uint64_t                numit1, numit2;
    
    it1 = reader1.First();
    it2 = reader2.First();
    numit1 = numit2 = 0;
    index = 0;
    dataPage = new StorageDataPage(mergeChunk, index);
    dataPage->SetOffset(offset);
    while(it1 != NULL || it2 != NULL)
    {
        advance1 = false;
        advance2 = false;

        skip = false;
        if (it1 != NULL && !RangeContains(firstKey, lastKey, it1->GetKey()))
        {
            advance1 = true;
            skip = true;
        }
        if (it2 != NULL && !RangeContains(firstKey, lastKey, it2->GetKey()))
        {
            advance2 = true;
            skip = true;
        }
        if (skip)
            goto Advance;

        if (it1 == NULL)
        {
            it = it2;
            advance2 = true;
        }
        else if (it2 == NULL)
        {
            it = it1;
            advance1 = true;
        }
        else
        {
            cmp = ReadBuffer::Cmp(it1->GetKey(), it2->GetKey());
            if (cmp < 0)
            {
                it = it1;
                advance1 = true;
            }
            else if (cmp > 0)
            {
                it = it2;
                advance2 = true;
            }
            else
            {
                it = Merge(it1, it2);
                // in case of delete 'it' is NULL
                advance1 = true;
                advance2 = true;
            }
        }

        if (it == NULL)
            goto Advance;

        numKeys++;
        
        if (firstKey.GetLength() == 0)
            this->firstKey.Write(it->GetKey());
        this->lastKey.Write(it->GetKey());

        if (mergeChunk->UseBloomFilter())
            mergeChunk->bloomPage->Add(it->GetKey());

        if (dataPage->GetNumKeys() == 0)
        {
            dataPage->Append(it);
            mergeChunk->indexPage->Append(it->GetKey(), index, offset);
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
                
                writeBuffer.Clear();
                dataPage->Write(writeBuffer);
                assert(writeBuffer.GetLength() == dataPage->GetSize());
                if (!WriteBuffer())
                    return false;

                mergeChunk->AppendDataPage(NULL);
                index++;
                delete dataPage;
                dataPage = new StorageDataPage(mergeChunk, index);
                dataPage->SetOffset(offset);
                dataPage->Append(it);
                mergeChunk->indexPage->Append(it->GetKey(), index, offset);
            }
        }
        
Advance:
        if (advance1)
        {
            it1 = reader1.Next(it1);
            numit1++;
        }
        if (advance2)
        {
            it2 = reader2.Next(it2);
            numit2++;
        }
    }

    // write last datapage
    if (dataPage->GetNumKeys() > 0)
    {
        dataPage->Finalize();

        writeBuffer.Clear();
        dataPage->Write(writeBuffer);
        assert(writeBuffer.GetLength() == dataPage->GetSize());
        if (!WriteBuffer())
            return false;

        mergeChunk->AppendDataPage(NULL);
        index++;
        delete dataPage;
    }
    
    mergeChunk->indexPage->Finalize();
    
    assert(numit1 = reader1.GetNumKeys());
    assert(numit2 = reader2.GetNumKeys());
    
    return true;
}

bool StorageChunkMerger::WriteIndexPage()
{
    mergeChunk->indexPage->SetOffset(offset);

    writeBuffer.Clear();
    mergeChunk->indexPage->Write(writeBuffer);
    assert(writeBuffer.GetLength() == mergeChunk->indexPage->GetSize());

    if (!WriteBuffer())
        return false;

    return true;
}

bool StorageChunkMerger::WriteBloomPage()
{
    mergeChunk->bloomPage->SetOffset(offset);

    writeBuffer.Clear();
    mergeChunk->bloomPage->Write(writeBuffer);
    assert(writeBuffer.GetLength() == mergeChunk->bloomPage->GetSize());

    if (!WriteBuffer())
        return false;

    return true;
}