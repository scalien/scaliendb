#include "StorageChunkMerger.h"

bool StorageChunkMerger::Merge(
 const char* rFilename1, const char* rFilename2, const char* wFilename
 uin64_t shardID, uint64_t chunkID, bool useBloomFilter,     
 Buffer& firstKey, Buffer& lastKey)
{
    // open readers
    if (!reader1.Open(rFilename1))
        return false;
    if (!reader2.Open(rFilename2))
        return false;

    if (!reader1.ReadHeaderPage())
        return false;
    if (!reader2.ReadHeaderPage())
        return false;

    // open writer
    if (fd.Open(wFilename, FS_READWRITE) == INVALID_FD)
        return false;

    offset = 0;
    numKeys = 0;

    indexPage.Clear();
    if (useBloomFilter)
    {
        bloomPage.Clear();
        bloomPage.SetNumKeys(reader1->GetNumKeys() + reader2->GetNumKeys());
    }

    if (!WriteEmptyHeaderPage())
        return false;

    if (!WriteDataPages(firstKey, lastKey))
        return false;

    if (!WriteIndexPage())
        return false;

    if (chunk->UseBloomFilter())
    {
        if (!WriteBloomPage())
            return false;
    }

    FS_Sync(fd.GetFD());

    FS_FileSeek(fd.GetFD(), 0, FS_SEEK_SET);
    offset = 0;

    if (!WriteHeader(shardID, chunkID, useBloomFilter))
        return false;

    FS_Sync(fd.GetFD());

    fd.Close();

    return true;
}

StorageKeyValue* StorageChunkMerger::Merge(StorageKeyValue* it1, StorageKeyValue* it2)
{
    if (it2->GetType() == STORAGE_KEYVALUE_SET)
        return it2;
    else if (it2->GetType() == STORAGE_KEYVALUE_DELETE)
        return NULL;
    else if (it2->GetType() == STORAGE_KEYVALUE_APPEND)
    {
        if (it1->GetType() == STORAGE_KEYVALUE_SET)
        {
            // merge the two values into a STORAGE_KEYVALUE_SET
            mergedKeyValue.Set(it1->GetKey(), it1->GetValue());
            mergedKeyValue.AppendToValue(it2->GetValue());
            return &mergedKeyValue;
        }
        else if (it1->GetType() == STORAGE_KEYVALUE_APPEND)
        {
            // merge the two values into a STORAGE_KEYVALUE_APPEND
            mergedKeyValue.Append(it1->GetKey(), it1->GetValue());
            mergedKeyValue.AppendToValue(it2->GetValue());
            return &mergedKeyValue;
        }
    }
    else
        ASSERT_FAIL();
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

bool StorageChunkMerger::WriteEmptyHeaderPage()
{
    uint32_t    pageSize;
    ssize_t     writeSize;

    pageSize = headerPage.GetPageSize();

    writeBuffer.Allocate(pageSize);
    writeBuffer.SetLength(pageSize);
    writeBuffer.Zero();

    if (!WriteBuffer())
        return false;

    return true;
}

bool StorageChunkMerger::WriteHeaderPage(uint64_t shardID, uint64_t chunkID, bool useBloomFilter)
{
    headerPage.SetShardID(shardID);
    headerPage.SetChunkID(chunkID);
    headerPage.SetLogSegmentID(MAX(reader1.GetLogSegmentID(), reader2.GetLogSegmentID()));
    headerPage.SetNumKeys(numKeys);

    headerPage.SetVersion(STORAGE_HEADER_PAGE_VERSION);
    headerPage.SetUseBloomFilter(useBloomFilter);
    headerPage.SetIndexPageOffset(indexPageOffset);
    headerPage.SetIndexPageSize(indexPageSize);
    headerPage.SetBloomPageOffset(bloomPageOffset);
    headerPage.SetBloomPageSize(bloomPageSize);

    headerPage.Write(writeBuffer);
    if (!WriteBuffer())
        return false;

    return true;
}

bool StorageChunkMerger::WriteDataPages(Buffer& firstKey, Buffer& lastKey)
{
    bool                advance1;
    bool                advance2;
    StorageKeyValue*    it1;
    StorageKeyValue*    it2;

    it1 = reader1.First();
    it2 = reader2.First();
    while(it1 != NULL || it2 != NULL)
    {
        advance1 = false;
        advance2 = false;

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
            if (it1 < it2)
            {
                it = it1;
                advance1 = true;
            }
            else if (it2 < it1)
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

        if (useBloomFilter)
            bloomPage.Add(it);

        if (dataPage.GetNumKeys() == 0)
        {
            dataPage.Append(it);
            indexPage.Append(it);
        }
        else
        {
            if (dataPage.GetLength() + dataPage.GetSizeIncrement(it) <= STORAGE_DEFAULT_DATA_PAGE_SIZE)
            {
                dataPage.Append(it);
            }
            else
            {
                dataPage.Write(writeBuffer);
                if (!WriteBuffer())
                    return false;
                dataPage.Clear();
            }
        }
        
Advance:
        if (advance1)
            it1 = reader1.Next(it1);
        if (advance2)
            it2 = reader2.Next(it2);
    }

    // write last datapage
    if (dataPage.GetNumKeys() > 0)
    {
        dataPage.Write(writeBuffer);
        if (!WriteBuffer())
            return false;
    }

    return true;
}

bool StorageChunkMerger::WriteIndexPage()
{
    indexPage.Write(writeBuffer);

    indexPageOffset = offset;
    indexPageSize = writeBuffer.GetLength();

    if (!WriteBuffer())
        return false;

    return true;
}

bool StorageChunkMerger::WriteBloomPage()
{
    bloomPage.Write(writeBuffer);

    bloomPageOffset = offset;
    bloomPageSize = writeBuffer.GetLength();

    if (!WriteBuffer())
        return false;

    return true;
}