#include "StorageChunkReader.h"

#define PRELOAD_THRESHOLD   (1*MB)

void StorageChunkReader::Open(ReadBuffer filename)
{
    fileChunk.useCache = false;
    fileChunk.SetFilename(filename);
    fileChunk.ReadHeaderPage();
    fileChunk.LoadIndexPage();
}

StorageFileKeyValue* StorageChunkReader::First()
{
    index = 0;
    prevIndex = 0;
    offset = STORAGE_HEADER_PAGE_SIZE;
    
    PreloadDataPages();
    
    return fileChunk.dataPages[index]->First();
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
    uint32_t    max;
    uint32_t    totalSize;
    
//    max = MIN(fileChunk.numDataPages, index + NUM_PRELOAD_PAGES);
//    preloadIndex = max - 1;
//    for (i = index; i < max; i++)
//    {
//        assert(fileChunk.dataPages[i] == NULL);
//        fileChunk.LoadDataPage(i, offset);
//        offset += fileChunk.dataPages[i]->GetSize();
//    }

    totalSize = 0;
    i = index;

    do
    {
        fileChunk.LoadDataPage(i, offset);
        offset += fileChunk.dataPages[i]->GetSize();
        totalSize += fileChunk.dataPages[i]->GetSize();
        preloadIndex = i;
        i++;
    }
    while (totalSize < PRELOAD_THRESHOLD);
}
