#include "StorageChunkReader.h"

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
    offset = STORAGE_HEADER_PAGE_SIZE;
    
    fileChunk.LoadDataPage(index, offset);
    
    return fileChunk.dataPages[index]->First();
}

StorageFileKeyValue* StorageChunkReader::Next(StorageFileKeyValue* it)
{
    StorageFileKeyValue* next;
    
    next = fileChunk.dataPages[index]->Next(it);
    
    if (next != NULL)
        return next;
    
    offset += fileChunk.dataPages[index]->GetSize();
//    delete fileChunk.dataPages[index];
//    fileChunk.dataPages[index] = NULL;

    index++;
    
    if (index >= fileChunk.numDataPages)
        return NULL;
        
    fileChunk.LoadDataPage(index, offset);
    
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
