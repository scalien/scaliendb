#include "StorageChunkReader.h"
#include "System/Time.h"

#define NUM_PRELOAD_PAGES   16

void StorageChunkReader::Open(ReadBuffer filename, uint64_t preloadThreshold_)
{
    fileChunk.useCache = false;
    fileChunk.SetFilename(filename);
    fileChunk.ReadHeaderPage();
    fileChunk.LoadIndexPage();

    preloadThreshold = preloadThreshold_;
}

StorageFileKeyValue* StorageChunkReader::First(ReadBuffer& firstKey)
{
    bool                    ret;
    int                     cmpres;
    StorageFileKeyValue*    it;
    
    prevIndex = 0;
    
    if (ReadBuffer::Cmp(firstKey, fileChunk.indexPage->GetFirstKey()) < 0)
    {
        index = 0;
        offset = STORAGE_HEADER_PAGE_SIZE;
    }
    else
    {
        ret = fileChunk.indexPage->Locate(firstKey, index, offset);
        ASSERT(ret);
    }
    
    PreloadDataPages();
    it = fileChunk.dataPages[index]->LocateKeyValue(firstKey, cmpres);
    if (it != NULL && cmpres > 0)
        it = Next(it);

    return it;
}

StorageFileKeyValue* StorageChunkReader::Next(StorageFileKeyValue* it)
{
    StorageFileKeyValue*    next;
    
    next = fileChunk.dataPages[index]->Next(it);
    
    if (next != NULL)
        return next;
    
    // keep the previous dataPage in memory
    if (index > prevIndex && fileChunk.dataPages[prevIndex] != NULL)
    {
        delete fileChunk.dataPages[prevIndex];
        fileChunk.dataPages[prevIndex] = NULL;
    }

    prevIndex = index;
    index++;

    if (index >= fileChunk.numDataPages)
        return NULL;

    if (index > preloadIndex)
        PreloadDataPages();

    return fileChunk.dataPages[index]->First();
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
    i = index;

    do
    {
        fileChunk.LoadDataPage(i, offset);
        pageSize = fileChunk.dataPages[i]->GetSize();
        offset += pageSize;
        totalSize += pageSize;
        preloadIndex = i;
        i++;
    }
    while (i < fileChunk.numDataPages && totalSize < preloadThreshold);
    
    MSleep(20);
}
