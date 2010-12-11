#include "StorageChunkFile.h"

StorageChunkFile::StorageChunkFile()
{
    size = 64;
    dataPages = (StorageDataPage**) malloc(sizeof(StorageDataPage*) * size);
    numDataPages = 0;
}

StorageChunkFile::~StorageChunkFile()
{
    free(dataPages);
}

void StorageChunkFile::AppendDataPage(StorageDataPage* dataPage)
{
    if (numDataPages == size)
        ExtendDataPageArray();
    
    dataPages[numDataPages] = dataPage;
    numDataPages++;
}

void StorageChunkFile::ExtendDataPageArray()
{
    StorageDataPage**   newDataPages;
    unsigned            newSize, i;
    
    newSize = size * 2;
    newDataPages = (StorageDataPage**) malloc(sizeof(StorageDataPage*) * newSize);
    
    for (i = 0; i < numDataPages; i++)
        newDataPages[i] = dataPages[i];
    
    free(dataPages);
    dataPages = newDataPages;
    size = newSize;
}
