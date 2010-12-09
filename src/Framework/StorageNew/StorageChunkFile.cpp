#include "StorageChunkFile.h"

StorageChunkFile::StorageChunkFile()
{
    size = 1024;
    dataPages = (StorageDataPage**) malloc(sizeof(StorageDataPage*) * size);
    length = 0;
}

StorageChunkFile::~StorageChunkFile()
{
    free(dataPages);
}

void StorageChunkFile::AppendDataPage(StorageDataPage* dataPage)
{
    if (length == size)
        ExtendDataPageArray();
    
    dataPages[length] = dataPage;
    length++;
}

void StorageChunkFile::ExtendDataPageArray()
{
    StorageDataPage**   newDataPages;
    unsigned            newSize, i;
    
    newSize = size * 2;
    newDataPages = (StorageDataPage**) malloc(sizeof(StorageDataPage*) * newSize);
    
    for (i = 0; i < length; i++)
        newDataPages[i] = dataPages[i];
    
    free(dataPages);
    dataPages = newDataPages;
    size = newSize;
}
