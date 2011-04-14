#include "System/FileSystem.h"
#include "StorageFileChunk.h"
#include "StorageBulkCursor.h"
#include "StoragePageCache.h"
#include "StorageEnvironment.h"
#include "StorageAsyncGet.h"

StorageFileChunk::StorageFileChunk() : headerPage(this)
{
    prev = next = this;
    written = false;
    dataPagesSize = 0;
    dataPages = NULL;
    indexPage = NULL;
    bloomPage = NULL;
    numDataPages = 0;
    fileSize = 0;
    useCache = true;
    deleted = false;
    isBloomPageLoading = false;
    isIndexPageLoading = false;
    fd = INVALID_FD;
    haveUncommitedWrites = false;
}

StorageFileChunk::~StorageFileChunk()
{
    unsigned    i;
    
    if (fd != INVALID_FD)
        FS_FileClose(fd);
    
    for (i = 0; i < numDataPages; i++)
    {
        if (dataPages[i] != NULL)
        {
//            if (dataPages[i]->IsCached())
//                StoragePageCache::RemovePage(dataPages[i]);

            delete dataPages[i];
        }
    }
    free(dataPages);
    
//    if (indexPage && indexPage->IsCached())
//        StoragePageCache::RemovePage(indexPage);
    delete indexPage;
    
//    if (bloomPage && bloomPage->IsCached())
//        StoragePageCache::RemovePage(bloomPage);
    delete bloomPage;
}

void StorageFileChunk::ReadHeaderPage()
{
    Buffer      buffer;
    uint64_t    offset;
    
    if (fd == INVALID_FD)
        OpenForReading();

    if (headerPage.GetChunkID() > 0)
        return; // already loaded

    offset = 0;
    
    if (!ReadPage(offset, buffer))
    {
        Log_Message("Unable to read header page from %s at offset %U", filename.GetBuffer(), offset);
        Log_Message("This should not happen.");
        Log_Message("Possible causes: software bug, damaged file, corrupted file...");
        Log_Message("Exiting...");
        STOP_FAIL(1);
    }
    
    if (!headerPage.Read(buffer))
    {
        Log_Message("Unable to parse header page read from %s at offset %U with size %u",
         filename.GetBuffer(), offset, buffer.GetLength());
        Log_Message("This should not happen.");
        Log_Message("Possible causes: software bug, damaged file, corrupted file...");
        Log_Message("Exiting...");
        STOP_FAIL(1);
    }
    
    fileSize = FS_FileSize(filename.GetBuffer());

    LoadIndexPage();
    if (UseBloomFilter())
        LoadBloomPage();
}

void StorageFileChunk::SetFilename(ReadBuffer filename_)
{
    filename.Write(filename_);
    filename.NullTerminate();

}

Buffer& StorageFileChunk::GetFilename()
{
    return filename;
}

bool StorageFileChunk::OpenForReading()
{
    Log_Debug("Opening readonly: %s", filename.GetBuffer());
    
    fd = FS_Open(filename.GetBuffer(), FS_READONLY);
    if (fd != INVALID_FD)
        return true;
    return false;
}

StorageChunk::ChunkState StorageFileChunk::GetChunkState()
{
    if (written)
        return StorageChunk::Written;
    else
        return StorageChunk::Unwritten;
}

void StorageFileChunk::NextBunch(StorageBulkCursor& cursor, StorageShard* shard)
{
    uint32_t                index;
    uint64_t                offset;
    ReadBuffer              nextKey, key, value;
    StorageFileKeyValue*    it;
    
    if (!shard->RangeContains(headerPage.GetFirstKey()) && !shard->RangeContains(headerPage.GetLastKey()))
    {
        cursor.SetLast(true);
        return;
    }
    nextKey = cursor.GetNextKey();

    if (indexPage == NULL)
        LoadIndexPage();

    if (!indexPage->Locate(nextKey, index, offset))
    {
        index = 0;
        offset = STORAGE_HEADER_PAGE_SIZE;
    }

    if (dataPages[index] == NULL)
        LoadDataPage(index, offset, true);
    
    for (unsigned i = 0; i < dataPages[index]->GetNumKeys(); i++)
    {
        it = dataPages[index]->GetIndexedKeyValue(i);
        if (!shard->RangeContains(it->GetKey()))
            continue;

        cursor.AppendKeyValue(it);
    }
    cursor.FinalizeKeyValues();
    
    if (index == (numDataPages - 1))
    {
        cursor.SetLast(true);
        return;
    }
    
    index++;
    if (dataPages[index] == NULL)
        LoadDataPage(index, dataPages[index - 1]->GetOffset() + dataPages[index - 1]->GetCompressedSize(), true);
    
    ASSERT(dataPages[index] != NULL);
    cursor.SetNextKey(dataPages[index]->GetIndexedKeyValue(0)->GetKey());
    cursor.SetLast(false);
}

uint64_t StorageFileChunk::GetChunkID()
{
    return headerPage.GetChunkID();
}

bool StorageFileChunk::UseBloomFilter()
{
    return headerPage.UseBloomFilter();
}

bool StorageFileChunk::IsMerged()
{
    return headerPage.IsMerged();
}

StorageKeyValue* StorageFileChunk::Get(ReadBuffer& key)
{
    uint32_t    index;
    uint64_t    offset;
    Buffer      buffer;

    if (headerPage.UseBloomFilter())
    {
        if (bloomPage == NULL)
            LoadBloomPage(); // evicted, load back
        if (bloomPage->IsCached())
            StoragePageCache::RegisterHit(bloomPage);
        if (!bloomPage->Check(key))
            return NULL;
    }

    if (indexPage == NULL)
        LoadIndexPage(); // evicted, load back
    if (indexPage->IsCached())
        StoragePageCache::RegisterHit(indexPage);
    if (!indexPage->Locate(key, index, offset))
        return NULL;
    
    if (dataPages[index] == NULL)
        LoadDataPage(index, offset); // evicted, load back

    if (dataPages[index]->IsCached())
        StoragePageCache::RegisterHit(dataPages[index]);
    return dataPages[index]->Get(key);
}

void StorageFileChunk::AsyncGet(StorageAsyncGet* asyncGet)
{
    uint32_t            index;
    uint64_t            offset;
    Buffer              buffer;
    StorageKeyValue*    kv;
    
    asyncGet->ret = false;

    if (headerPage.UseBloomFilter())
    {
        if (bloomPage == NULL)
        {
            asyncGet->stage = StorageAsyncGet::BLOOM_PAGE;
            asyncGet->threadPool->Execute(
             MFunc<StorageAsyncGet, &StorageAsyncGet::AsyncLoadPage>(asyncGet)); // evicted, load back
            return;
        }
        if (bloomPage->IsCached())
            StoragePageCache::RegisterHit(bloomPage);
        if (!bloomPage->Check(asyncGet->key))
        {
            asyncGet->completed = true;
            return;
        }
    }

    if (indexPage == NULL)
    {
        asyncGet->stage = StorageAsyncGet::INDEX_PAGE;
        asyncGet->threadPool->Execute(
         MFunc<StorageAsyncGet, &StorageAsyncGet::AsyncLoadPage>(asyncGet)); // evicted, load back
        return;
    }
    if (indexPage->IsCached())
        StoragePageCache::RegisterHit(indexPage);
    if (!indexPage->Locate(asyncGet->key, index, offset))
    {
        asyncGet->completed = true;
        return;
    }
    
    if (dataPages[index] == NULL)
    {
        asyncGet->stage = StorageAsyncGet::DATA_PAGE;
        asyncGet->index = index;
        asyncGet->offset = offset;
        asyncGet->threadPool->Execute(
         MFunc<StorageAsyncGet, &StorageAsyncGet::AsyncLoadPage>(asyncGet)); // evicted, load back
        return;
    }

    if (dataPages[index]->IsCached())
        StoragePageCache::RegisterHit(dataPages[index]);

    kv = dataPages[index]->Get(asyncGet->key);
    if (kv == NULL || kv->GetType() == STORAGE_KEYVALUE_TYPE_DELETE)
    {
        asyncGet->completed = true;
        return;
    }

    asyncGet->value = kv->GetValue();
    asyncGet->ret = true;
    asyncGet->completed = true;
}

uint64_t StorageFileChunk::GetMinLogSegmentID()
{
    return headerPage.GetMinLogSegmentID();
}

uint64_t StorageFileChunk::GetMaxLogSegmentID()
{
    return headerPage.GetMaxLogSegmentID();
}

uint32_t StorageFileChunk::GetMaxLogCommandID()
{
    return headerPage.GetMaxLogCommandID();
}

ReadBuffer StorageFileChunk::GetFirstKey()
{
    return headerPage.GetFirstKey();
}

ReadBuffer StorageFileChunk::GetLastKey()
{
    return headerPage.GetLastKey();
}

uint64_t StorageFileChunk::GetSize()
{
    return fileSize;
}

uint64_t StorageFileChunk::GetPartialSize(ReadBuffer firstKey, ReadBuffer lastKey)
{
    uint64_t    firstOffset;
    uint64_t    lastOffset;
    uint32_t    index;
    
    // TODO: ensure that index page is loaded
    if (indexPage == NULL)
        return fileSize;

    firstOffset = 0;
    indexPage->Locate(firstKey, index, firstOffset);
    
    lastOffset = fileSize;
    indexPage->Locate(lastKey, index, lastOffset);
    
    return lastOffset - firstOffset;
}

ReadBuffer StorageFileChunk::GetMidpoint()
{
    return headerPage.GetMidpoint();
}

void StorageFileChunk::AddPagesToCache()
{
    uint32_t i;
    
    for (i = 0; i < numDataPages; i++)
    {
        if (dataPages[i] != NULL)
            StoragePageCache::AddPage(dataPages[i], true);
    }

    if (UseBloomFilter() && bloomPage != NULL)
        StoragePageCache::AddPage(bloomPage);

    if (indexPage != NULL)
        StoragePageCache::AddPage(indexPage);
}

void StorageFileChunk::RemovePagesFromCache()
{
    uint32_t i;
    
    if (UseBloomFilter() && bloomPage != NULL)
        StoragePageCache::RemovePage(bloomPage);

    if (indexPage != NULL)
        StoragePageCache::RemovePage(indexPage);

    for (i = 0; i < numDataPages; i++)
    {
        if (dataPages[i] != NULL)
            StoragePageCache::RemovePage(dataPages[i]);
    }
}

void StorageFileChunk::OnBloomPageEvicted()
{
    delete bloomPage;
    bloomPage = NULL;
}

void StorageFileChunk::OnIndexPageEvicted()
{
    Log_Debug("Index page evicted");
    delete indexPage;
    indexPage = NULL;
}

void StorageFileChunk::OnDataPageEvicted(uint32_t index)
{
    ASSERT(dataPages[index] != NULL);
    
    delete dataPages[index];
    dataPages[index] = NULL;
}

void StorageFileChunk::LoadBloomPage()
{
    Buffer      buffer;
    uint64_t    offset;
    
    if (fd == INVALID_FD)
        OpenForReading();
    
    bloomPage = new StorageBloomPage(this);
    offset = headerPage.GetBloomPageOffset();
    bloomPage->SetOffset(offset);
    if (!ReadPage(offset, buffer))
    {
        Log_Message("Unable to read bloom page from %s at offset %U", filename.GetBuffer(), offset);
        Log_Message("This should not happen.");
        Log_Message("Possible causes: software bug, damaged file, corrupted file...");
        Log_Message("Exiting...");
        STOP_FAIL(1);
    }
    if (!bloomPage->Read(buffer))
    {
        Log_Message("Unable to parse bloom page read from %s at offset %U with size %u",
         filename.GetBuffer(), offset, buffer.GetLength());
        Log_Message("This should not happen.");
        Log_Message("Possible causes: software bug, damaged file, corrupted file...");
        Log_Message("Exiting...");
        STOP_FAIL(1);
    }
    
    if (useCache)
        StoragePageCache::AddPage(bloomPage);
}

void StorageFileChunk::LoadIndexPage()
{
    Buffer      buffer;
    uint64_t    offset;

    if (fd == INVALID_FD)
        OpenForReading();
    
    indexPage = new StorageIndexPage(this);
    offset = headerPage.GetIndexPageOffset();
    indexPage->SetOffset(offset);
    if (!ReadPage(offset, buffer))
    {
        Log_Message("Unable to read index page from %s at offset %U", filename.GetBuffer(), offset);
        Log_Message("This should not happen.");
        Log_Message("Possible causes: software bug, damaged file, corrupted file...");
        Log_Message("Exiting...");
        STOP_FAIL(1);
    }
    if (!indexPage->Read(buffer))
    {
        Log_Message("Unable to parse index page read from %s at offset %U with size %u",
         filename.GetBuffer(), offset, buffer.GetLength());
        Log_Message("This should not happen.");
        Log_Message("Possible causes: software bug, damaged file, corrupted file...");
        Log_Message("Exiting...");
        STOP_FAIL(1);
    }
    
    if (useCache)
        StoragePageCache::AddPage(indexPage);
    
    // TODO: HACK
    if (numDataPages == 0)
    {
        numDataPages = indexPage->GetNumDataPages();
        StorageDataPage** newDataPages;
        free(dataPages);
        newDataPages = (StorageDataPage**) malloc(sizeof(StorageDataPage*) * numDataPages);
        
        for (unsigned i = 0; i < numDataPages; i++)
            newDataPages[i] = NULL;
        
        free(dataPages);
        dataPages = newDataPages;
        dataPagesSize = numDataPages;
    }
}

void StorageFileChunk::LoadDataPage(uint32_t index, uint64_t offset, bool bulk, bool keysOnly)
{
    Buffer buffer;

    if (fd == INVALID_FD)
        OpenForReading();
    
    dataPages[index] = new StorageDataPage(this, index);
    dataPages[index]->SetOffset(offset);
    if (!ReadPage(offset, buffer, keysOnly))
    {
        Log_Message("Unable to read data page from %s at offset %U", filename.GetBuffer(), offset);
        Log_Message("This should not happen.");
        Log_Message("Possible causes: software bug, damaged file, corrupted file...");
        Log_Message("Exiting...");
        STOP_FAIL(1);
    }
    if (!dataPages[index]->Read(buffer, keysOnly))
    {
        Log_Message("Unable to parse data page read from %s at offset %U with size %u",
         filename.GetBuffer(), offset, buffer.GetLength());
        Log_Message("This should not happen.");
        Log_Message("Possible causes: software bug, damaged file, corrupted file...");
        Log_Message("Exiting...");
        STOP_FAIL(1);
    }

    ASSERT(dataPages[index] != NULL);
    
    if (useCache)
        StoragePageCache::AddPage(dataPages[index], bulk);
    
    ASSERT(dataPages[index] != NULL);
}

StoragePage* StorageFileChunk::AsyncLoadBloomPage()
{
    Buffer              buffer;
    uint64_t            offset;
    StorageBloomPage*   page;
    
    if (isBloomPageLoading || bloomPage != NULL)
        return NULL;
    
    Log_Debug("async loading bloom page: %U, cache size: %U, cache num: %u", GetChunkID(), StoragePageCache::GetSize(), StoragePageCache::GetNumPages());

    if (fd == INVALID_FD)
        OpenForReading();
    
    isBloomPageLoading = true;
    page = new StorageBloomPage(this);
    offset = headerPage.GetBloomPageOffset();
    page->SetOffset(offset);
    if (!ReadPage(offset, buffer))
    {
        Log_Message("Unable to read bloom page from %s at offset %U", filename.GetBuffer(), offset);
        Log_Message("This should not happen.");
        Log_Message("Possible causes: software bug, damaged file, corrupted file...");
        Log_Message("Exiting...");
        STOP_FAIL(1);
    }
    if (!page->Read(buffer))
    {
        Log_Message("Unable to parse bloom page read from %s at offset %U with size %u",
         filename.GetBuffer(), offset, buffer.GetLength());
        Log_Message("This should not happen.");
        Log_Message("Possible causes: software bug, damaged file, corrupted file...");
        Log_Message("Exiting...");
        STOP_FAIL(1);
    }
    
    Log_Debug("async loading finished, bloom page: %U", GetChunkID());    
    
    return page;
}

StoragePage* StorageFileChunk::AsyncLoadIndexPage()
{
    Buffer              buffer;
    uint64_t            offset;
    StorageIndexPage*   page;
    
    if (isIndexPageLoading || indexPage != NULL)
        return NULL;

    Log_Debug("async loading index page: %U, cache size: %U, cache num: %u", GetChunkID(), StoragePageCache::GetSize(), StoragePageCache::GetNumPages());
    
    if (fd == INVALID_FD)
        OpenForReading();

    isIndexPageLoading = true;
    page = new StorageIndexPage(this);
    offset = headerPage.GetIndexPageOffset();
    page->SetOffset(offset);
    if (!ReadPage(offset, buffer))
    {
        Log_Message("Unable to read index page from %s at offset %U", filename.GetBuffer(), offset);
        Log_Message("This should not happen.");
        Log_Message("Possible causes: software bug, damaged file, corrupted file...");
        Log_Message("Exiting...");
        STOP_FAIL(1);
    }
    if (!page->Read(buffer))
    {
        Log_Message("Unable to parse index page read from %s at offset %U with size %u",
         filename.GetBuffer(), offset, buffer.GetLength());
        Log_Message("This should not happen.");
        Log_Message("Possible causes: software bug, damaged file, corrupted file...");
        Log_Message("Exiting...");
        STOP_FAIL(1);
    }

    Log_Debug("async loading finished, index page: %U", GetChunkID());

    return page;    
}

StoragePage* StorageFileChunk::AsyncLoadDataPage(uint32_t index, uint64_t offset)
{
    Buffer              buffer;
    StorageDataPage*    page;
    
    if (dataPages[index] != NULL)
        return NULL;
    
//    Log_Debug("async loading data page, chunk:%u, index: %u", headerPage.GetChunkID(), index);

    if (fd == INVALID_FD)
        OpenForReading();
    
    page = new StorageDataPage(this, index);
    page->SetOffset(offset);
    if (!ReadPage(offset, buffer))
    {
        Log_Message("Unable to read data page from %s at offset %U", filename.GetBuffer(), offset);
        Log_Message("This should not happen.");
        Log_Message("Possible causes: software bug, damaged file, corrupted file...");
        Log_Message("Exiting...");
        STOP_FAIL(1);
    }
    if (!page->Read(buffer))
    {
        Log_Message("Unable to parse data page read from %s at offset %U with size %u",
         filename.GetBuffer(), offset, buffer.GetLength());
        Log_Message("This should not happen.");
        Log_Message("Possible causes: software bug, damaged file, corrupted file...");
        Log_Message("Exiting...");
        STOP_FAIL(1);
    }

    return page;
}

bool StorageFileChunk::RangeContains(ReadBuffer key)
{
    int         cf, cl;
    ReadBuffer  firstKey, lastKey;

    firstKey = headerPage.GetFirstKey();
    lastKey = headerPage.GetLastKey();

    cf = ReadBuffer::Cmp(firstKey, key);
    cl = ReadBuffer::Cmp(key, lastKey);

    if (firstKey.GetLength() == 0)
    {
        if (lastKey.GetLength() == 0)
            return false;
        else
            return (cl <= 0);        // (key < lastKey);
    }
    else if (lastKey.GetLength() == 0)
    {
        return (cf <= 0);           // (firstKey <= key);
    }
    else
        return (cf <= 0 && cl <= 0); // (firstKey <= key <= lastKey);
}

void StorageFileChunk::AppendDataPage(StorageDataPage* dataPage)
{
    if (numDataPages == dataPagesSize)
        ExtendDataPageArray();
    
    dataPages[numDataPages] = dataPage;
    numDataPages++;
}

void StorageFileChunk::AllocateDataPageArray()
{
    numDataPages = indexPage->GetNumDataPages();
    StorageDataPage** newDataPages;
    newDataPages = (StorageDataPage**) malloc(sizeof(StorageDataPage*) * numDataPages);
    
    for (unsigned i = 0; i < numDataPages; i++)
        newDataPages[i] = NULL;
    
    free(dataPages);
    dataPages = newDataPages;
    dataPagesSize = numDataPages;
}

void StorageFileChunk::ExtendDataPageArray()
{
    StorageDataPage**   newDataPages;
    unsigned            newSize, i;
    
    if (dataPagesSize == 0)
        dataPagesSize = 32;
        
    newSize = dataPagesSize * 2;
    newDataPages = (StorageDataPage**) malloc(sizeof(StorageDataPage*) * newSize);
    
    for (i = 0; i < numDataPages; i++)
        newDataPages[i] = dataPages[i];
    
    free(dataPages);
    dataPages = newDataPages;
    dataPagesSize = newSize;
}

bool StorageFileChunk::ReadPage(uint64_t offset, Buffer& buffer, bool keysOnly)
{
    uint32_t    size, keysSize, rest;
    ssize_t     nread;
    ReadBuffer  parse;
    
    size = STORAGE_DEFAULT_PAGE_GRAN;
    buffer.Allocate(size);
    if ((nread = FS_FileReadOffs(fd, buffer.GetBuffer(), size, offset)) != size)
        return false;
    if (nread < 4)
        return false;
        
    buffer.SetLength(nread);
    // first 4 bytes on all pages is the page size
    parse.Wrap(buffer);
    if (!parse.ReadLittle32(size))
        return false;

    if (keysOnly)
    {
        // read only keys
        parse.Advance(8);
        if (!parse.ReadLittle32(keysSize))
            return false;
        size = 16 + keysSize;
    }
    
    if (size <= nread)
    {
        buffer.SetLength(size);
        return true;
    }

    rest = size - buffer.GetLength();
    if (rest > 0)
    {
        // read rest
        buffer.Allocate(size);
        if (FS_FileReadOffs(fd, buffer.GetPosition(), rest, offset + buffer.GetLength()) != rest)
            return false;
        buffer.SetLength(size);
    }
    
    return true;
}
