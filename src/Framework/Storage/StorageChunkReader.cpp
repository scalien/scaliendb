#include "StorageChunkReader.h"
#include "System/Time.h"

void StorageChunkReader::Open(
 ReadBuffer filename, uint64_t preloadThreshold_,
 bool keysOnly_, bool forwardDirection_)
{
    forwardDirection = forwardDirection_;
    preloadThreshold = preloadThreshold_;
    keysOnly = keysOnly_;

    fileChunk.useCache = false;
    fileChunk.SetFilename(filename);
    fileChunk.ReadHeaderPage();
    fileChunk.LoadIndexPage();

}

StorageFileKeyValue* StorageChunkReader::First(ReadBuffer& firstKey)
{
    bool                    ret;
    int                     cmpres;
    StorageFileKeyValue*    it;
    
    prevIndex = 0;

    if (forwardDirection && ReadBuffer::Cmp(firstKey, fileChunk.indexPage->GetFirstKey()) < 0)
    {
        index = 0;
        offset = fileChunk.indexPage->GetFirstDatapageOffset();
    }
    else if (!forwardDirection && firstKey.GetLength() > 0 && ReadBuffer::Cmp(firstKey, fileChunk.indexPage->GetFirstKey()) < 0)
    {
        // firstKey is set and smaller that the first key in this chunk
        return NULL;
    }
    else if (!forwardDirection && firstKey.GetLength() > 0 && ReadBuffer::Cmp(firstKey, fileChunk.indexPage->GetLastKey()) > 0)
    {
        index = fileChunk.numDataPages - 1;
        offset = fileChunk.indexPage->GetLastDatapageOffset();
    }
    else if (!forwardDirection && firstKey.GetLength() == 0)
    {
        index = fileChunk.numDataPages - 1;
        offset = fileChunk.indexPage->GetLastDatapageOffset();
    }
    else
    {
        ret = fileChunk.indexPage->Locate(firstKey, index, offset);
        ASSERT(ret);
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
}
