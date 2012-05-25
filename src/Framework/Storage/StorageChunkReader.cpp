#include "StorageChunkReader.h"
#include "System/Time.h"
#include "System/IO/IOProcessor.h"

StorageChunkReader::~StorageChunkReader()
{
    if (preloadThreshold == 0)
    {
        if (prevIndex < fileChunk.numDataPages)
        {
            if (fileChunk.dataPages[prevIndex] != NULL)
            {
                StorageDataPageCache::Release(fileChunk.dataPages[prevIndex]);
                fileChunk.dataPages[prevIndex] = NULL;
            }
        }

        if (index < fileChunk.numDataPages)
        {
            if (fileChunk.dataPages[index] != NULL)
            {
                StorageDataPageCache::Release(fileChunk.dataPages[index]);
                fileChunk.dataPages[index] = NULL;
            }
        }
    }
}

void StorageChunkReader::Open(
 ReadBuffer filename, uint64_t preloadThreshold_,
 bool keysOnly_, bool forwardDirection_)
{
    count = 0;
    numRead = 0;
    isLocated = false;

    forwardDirection = forwardDirection_;
    preloadThreshold = preloadThreshold_;
    keysOnly = keysOnly_;

    fileChunk.useCache = false;
    fileChunk.SetFilename(filename);
    fileChunk.ReadHeaderPage();
    fileChunk.LoadIndexPage();

    indexPage = fileChunk.indexPage;
    numDataPages = fileChunk.numDataPages;

    index = 0;
    offset = 0;
}

void StorageChunkReader::OpenWithFileChunk(
 StorageFileChunk* fileChunk_, ReadBuffer& firstKey_, uint64_t preloadThreshold_,
 bool keysOnly_, bool forwardDirection_)
{
    count = 0;
    numRead = 0;
    isLocated = false;

    forwardDirection = forwardDirection_;
    preloadThreshold = preloadThreshold_;
    if (forwardDirection == false)
        preloadThreshold = 0;   // don't preload when iterating backwards
    keysOnly = keysOnly_;

    indexPage = fileChunk_->indexPage;
    numDataPages = fileChunk_->numDataPages;

    fileChunk.useCache = false;
    fileChunk.SetFilename(fileChunk_->GetFilename());
    // it is safe to shallow copy headerPage
    fileChunk.headerPage = fileChunk_->headerPage;
    fileChunk.SetNumDataPages(numDataPages);

    index = 0;
    offset = 0;

    if (indexPage != NULL)
    {
        isLocated = true;
        LocateIndexAndOffset(indexPage, numDataPages, firstKey_);
    }
}

void StorageChunkReader::SetEndKey(ReadBuffer endKey_)
{
    endKey = endKey_;
}

void StorageChunkReader::SetPrefix(ReadBuffer prefix_)
{
    prefix = prefix_;
}

void StorageChunkReader::SetCount(unsigned count_)
{
    count = count_;
}

StorageFileKeyValue* StorageChunkReader::First(ReadBuffer& firstKey)
{
    int                     cmpres;
    bool                    ret;
    StorageFileKeyValue*    it;
    
    if (isLocated && offset == 0)
        return NULL;

    if (!isLocated)
    {
        isLocated = true;
        if (fileChunk.indexPage == NULL)
        {
            Log_Debug("indexPage was not loaded");
            fileChunk.ReadHeaderPage();
            fileChunk.LoadIndexPage();
        }
        ret = LocateIndexAndOffset(fileChunk.indexPage, fileChunk.numDataPages, firstKey);
        if (ret == false)
            return NULL;
    }

    if (fileChunk.dataPages == NULL)
        fileChunk.SetNumDataPages(numDataPages);

    prevIndex = 0;
    PreloadDataPages();

    if (!forwardDirection && firstKey.GetLength() == 0)
    {
        it = fileChunk.dataPages[index]->Last();
        if (it != NULL && prefix.GetLength() > 0 && !it->GetKey().BeginsWith(prefix))
            it = NULL;
        return it;
    }

    it = fileChunk.dataPages[index]->LocateKeyValue(firstKey, cmpres);

    if (it != NULL)
    {
        if (forwardDirection && cmpres > 0)
            it = Next(it);
        else if (!forwardDirection && cmpres < 0)
            it = Next(it);

        // TODO: FIXME: this is an optimization that only works when iterating forwards
        // TODO: make it work for reverse iteration too
        if (it != NULL && prefix.GetLength() > 0 && !it->GetKey().BeginsWith(prefix))
            it = NULL;
    }

    return it;
}

StorageFileKeyValue* StorageChunkReader::Next(StorageFileKeyValue* it)
{
    StorageFileKeyValue*    next;
    
    if (forwardDirection)
    {
        next = fileChunk.dataPages[index]->Next(it);
        if (next != NULL)
        {
            if (prefix.GetLength() > 0 && !next->GetKey().BeginsWith(prefix))
                next = NULL;
            return next;
        }
        if (index > prevIndex && fileChunk.dataPages[prevIndex] != NULL)
        {
            if (preloadThreshold > 0)
                delete fileChunk.dataPages[prevIndex];
            else
                StorageDataPageCache::Release(fileChunk.dataPages[prevIndex]);

            fileChunk.dataPages[prevIndex] = NULL;
        }
        prevIndex = index;
        index++;
        if (index == fileChunk.numDataPages)
            return NULL;
        if (index > preloadIndex)
            PreloadDataPages();
        
        next = fileChunk.dataPages[index]->First();
        if (prefix.GetLength() > 0 && !next->GetKey().BeginsWith(prefix))
            next = NULL;
        return next;
    }
    else
    {
        next = fileChunk.dataPages[index]->Prev(it);
        if (next != NULL)
        {
            if (prefix.GetLength() > 0 && !next->GetKey().BeginsWith(prefix))
                next = NULL;
            return next;
        }
        if (index < prevIndex && fileChunk.dataPages[prevIndex] != NULL)
        {
            // backward iteration happens only in list, never in merge
            StorageDataPageCache::Release(fileChunk.dataPages[prevIndex]);
            fileChunk.dataPages[prevIndex] = NULL;
        }
        prevIndex = index;
        if (index == 0)
            return NULL;
        index--;
        if (index < preloadIndex)
            PreloadDataPages();
        next = fileChunk.dataPages[index]->Last();
        if (prefix.GetLength() > 0 && !next->GetKey().BeginsWith(prefix))
            next = NULL;
        return next;
    }
}

StorageDataPage* StorageChunkReader::FirstDataPage()
{
    ASSERT(forwardDirection);

    prevIndex = 0;
    index = 0;
    offset = STORAGE_HEADER_PAGE_SIZE;

    PreloadDataPages();
    return fileChunk.dataPages[index];
}

StorageDataPage* StorageChunkReader::NextDataPage()
{
    ASSERT(forwardDirection);

    if (preloadThreshold > 0)
        delete fileChunk.dataPages[prevIndex];
    else
        StorageDataPageCache::Release(fileChunk.dataPages[prevIndex]);

    fileChunk.dataPages[prevIndex] = NULL;
    
    prevIndex = index;
    index++;
    
    if (index >= fileChunk.numDataPages)
        return NULL;
    
    if (index > preloadIndex)
        PreloadDataPages();
    
    return fileChunk.dataPages[index];
}

uint64_t StorageChunkReader::GetNumKeys()
{
    return fileChunk.headerPage.GetNumKeys();
}

uint64_t StorageChunkReader::GetMinLogSegmentID()
{
    return fileChunk.headerPage.GetMinLogSegmentID();
}

uint64_t StorageChunkReader::GetMaxLogSegmentID()
{
    return fileChunk.headerPage.GetMaxLogSegmentID();
}

uint64_t StorageChunkReader::GetMaxLogCommandID()
{
    return fileChunk.headerPage.GetMaxLogCommandID();
}

void StorageChunkReader::PreloadDataPages()
{
    uint32_t    i;
    uint32_t    pageSize;
    uint64_t    totalSize;
    
    totalSize = 0;
    
    if (forwardDirection)
    {
        i = index;
        do
        {
            if (preloadThreshold > 0)
            {
                // merge case
                fileChunk.LoadDataPage(i, offset, false, keysOnly);
            }
            else
            {
                // forward list case
                fileChunk.LoadDataPage(i, offset, false, keysOnly, StorageDataPageCache::Acquire());
            }

            //Log_Debug("Preloading datapage %u at offset %U from chunk %U", i, offset, fileChunk.GetChunkID());
            pageSize = fileChunk.dataPages[i]->GetSize();
            totalSize += pageSize;
            offset += pageSize;
            i++;
        }
        while (i > 0 && i < fileChunk.numDataPages && totalSize < preloadThreshold);

        preloadIndex = i-1;
    }
    else
    {
        // The offset and index is already located, so we don't need to
        // load the header and index page
        if (prevIndex != 0)
        {
            // don't preload when going backwards
            if (fileChunk.indexPage == NULL)
            {
                fileChunk.ReadHeaderPage();
                fileChunk.LoadIndexPage();
            }

            offset = fileChunk.indexPage->GetIndexOffset(index);
        }
        // backward iteration happens only in list, never in merge
        fileChunk.LoadDataPage(index, offset, false, keysOnly, StorageDataPageCache::Acquire());
        pageSize = fileChunk.dataPages[index]->GetSize();
        totalSize += pageSize;
        preloadIndex = index;
    }
}

bool StorageChunkReader::LocateIndexAndOffset(StorageIndexPage* indexPage, uint32_t numDataPages, ReadBuffer& firstKey)
{
    bool    ret;

    if (forwardDirection && ReadBuffer::Cmp(firstKey, indexPage->GetFirstKey()) < 0)
    {
        index = 0;
        offset = indexPage->GetFirstDatapageOffset();
    }
    else if (!forwardDirection && firstKey.GetLength() > 0 && ReadBuffer::Cmp(firstKey, indexPage->GetFirstKey()) < 0)
    {
        // firstKey is set and smaller that the first key in this chunk
        return false;
    }
    else if (!forwardDirection && firstKey.GetLength() > 0 && ReadBuffer::Cmp(firstKey, indexPage->GetLastKey()) > 0)
    {
        index = numDataPages - 1;
        offset = indexPage->GetLastDatapageOffset();
    }
    else if (!forwardDirection && firstKey.GetLength() == 0)
    {
        index = numDataPages - 1;
        offset = indexPage->GetLastDatapageOffset();
    }
    else
    {
        ret = indexPage->Locate(firstKey, index, offset);
        ASSERT(ret);
    }

    return true;
}
