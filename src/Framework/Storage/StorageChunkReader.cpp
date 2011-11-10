#include "StorageChunkReader.h"
#include "System/Time.h"
#include "System/IO/IOProcessor.h"

void StorageChunkReader::Open(
 ReadBuffer filename, uint64_t preloadThreshold_,
 bool keysOnly_, bool forwardDirection_)
{
    count = 0;
    numRead = 0;

    forwardDirection = forwardDirection_;
    preloadThreshold = preloadThreshold_;
    keysOnly = keysOnly_;

    fileChunk.useCache = false;
    fileChunk.SetFilename(filename);
    fileChunk.ReadHeaderPage();
    fileChunk.LoadIndexPage();

    indexPage = fileChunk.indexPage;
    numDataPages = fileChunk.numDataPages;
}

void StorageChunkReader::OpenWithFileChunk(
 StorageFileChunk* fileChunk_, uint64_t preloadThreshold_,
 bool keysOnly_, bool forwardDirection_)
{
    count = 0;
    numRead = 0;

    forwardDirection = forwardDirection_;
    preloadThreshold = preloadThreshold_;
    keysOnly = keysOnly_;

    indexPage = fileChunk_->indexPage;
    numDataPages = fileChunk_->numDataPages;

    fileChunk.useCache = false;
    fileChunk.SetFilename(fileChunk_->GetFilename());
    // TODO: this is safe to copy, only owner points to bad fileChunk but it is never used
    fileChunk.headerPage = fileChunk_->headerPage;
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

StorageFileKeyValue* StorageChunkReader::First(ReadBuffer& firstKey_)
{
    int                     cmpres;
    bool                    ret;
    StorageFileKeyValue*    it;
    Callable                onLocateIndexAndOffset;
    
    index = 0;
    offset = 0;

    prevIndex = 0;
    firstKey = firstKey_;

    // TODO: 
    // if (index page is loaded)
    //      go back to main thread
    //      try to determine value of index member variable
    //      if (success)
    //        return to listing thread with index value set with success flag
    //      else
    //          return to listing thread with failure flag
    //          load index page
    //          locate index and offset value
    // else // index page is not loaded
    //      load index page
    //      locate index and offset value
    if (indexPage && fileChunk.indexPage == NULL)
    {
        onLocateIndexAndOffset = MFUNC(StorageChunkReader, OnLocateIndexAndOffset);
        signal.SetWaiting(true);
        IOProcessor::Complete(&onLocateIndexAndOffset);
        signal.Wait();

        // the indexPage was evicted from cache on the main thread
        if (indexPage == NULL)
        {
            Log_Message("indexPage was evicted in main thread");
            fileChunk.ReadHeaderPage();
            fileChunk.LoadIndexPage();
            ret = LocateIndexAndOffset(fileChunk.indexPage, fileChunk.numDataPages, firstKey);
        }
    }
    else
    {
        if (fileChunk.indexPage == NULL)
        {
            Log_Message("indexPage was not loaded");
            fileChunk.ReadHeaderPage();
            fileChunk.LoadIndexPage();
        }
        ret = LocateIndexAndOffset(fileChunk.indexPage, fileChunk.numDataPages, firstKey);
    }

    if (offset == 0)
        return NULL;

    if (fileChunk.dataPages == NULL)
    {
        fileChunk.numDataPages = numDataPages;
        fileChunk.AllocateDataPageArray();
    }

    PreloadDataPages();

    if (!forwardDirection && firstKey.GetLength() == 0)
    {
        return fileChunk.dataPages[index]->Last();
    }

    it = fileChunk.dataPages[index]->LocateKeyValue(firstKey, cmpres);

    if (it != NULL)
    {
        if (forwardDirection && cmpres > 0)
            it = Next(it);
        else if (!forwardDirection && cmpres < 0)
            it = Next(it);
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
            return next;
        if (index > prevIndex && fileChunk.dataPages[prevIndex] != NULL)
        {
            delete fileChunk.dataPages[prevIndex];
            fileChunk.dataPages[prevIndex] = NULL;
        }
        prevIndex = index;
        index++;
        if (index == fileChunk.numDataPages)
            return NULL;
        if (index > preloadIndex)
            PreloadDataPages();
        return fileChunk.dataPages[index]->First();
    }
    else
    {
        next = fileChunk.dataPages[index]->Prev(it);
        if (next != NULL)
            return next;
        if (index < prevIndex && fileChunk.dataPages[prevIndex] != NULL)
        {
            delete fileChunk.dataPages[prevIndex];
            fileChunk.dataPages[prevIndex] = NULL;
        }
        prevIndex = index;
        if (index == 0)
            return NULL;
        index--;
        if (index < preloadIndex)
            PreloadDataPages();
        return fileChunk.dataPages[index]->Last();
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

    delete fileChunk.dataPages[prevIndex];
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
    uint64_t    origOffset;
    
    Log_Debug("PreloadDataPages started");

    totalSize = 0;
    origOffset = 0; // to make the compiler happy

    if (forwardDirection)
        i = index;
    else
    {
        if (offset > preloadThreshold)
            offset -= preloadThreshold;
        else
            offset = 0;
        preloadIndex = i = fileChunk.indexPage->GetOffsetIndex(offset);
        origOffset = offset;
    }

    do
    {
        fileChunk.LoadDataPage(i, offset, false, keysOnly);
        //Log_Debug("Preloading datapage %u at offset %U from chunk %U", i, offset, fileChunk.GetChunkID());
        pageSize = fileChunk.dataPages[i]->GetSize();
        totalSize += pageSize;
        offset += pageSize;
        i++;
    }
    while (i > 0 && i < fileChunk.numDataPages &&
           (((forwardDirection && totalSize < preloadThreshold)) || (!forwardDirection && i <= index)));
    
    if (forwardDirection)
        preloadIndex = i-1;
    else
        offset = origOffset;

    Log_Debug("PreloadDataPages finished");
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

void StorageChunkReader::OnLocateIndexAndOffset()
{
    // TODO: indexPage may be evicted
    if (indexPage == NULL)
    {
        // indexPage is already evicted
        indexPage = NULL;
        signal.Wake();
        return;
    }

    LocateIndexAndOffset(indexPage, numDataPages, firstKey);
    signal.Wake();
}
