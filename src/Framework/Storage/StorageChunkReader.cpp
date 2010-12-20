#include "StorageChunkReader.h"

bool StorageChunkReader::Open(const char* filename)
{
    if (fd.Open(filename, FS_READONLY) == INVALID_FD)
        return false;
    offset = 0;
}

void StorageChunkReader::Close()
{
    fd.Close();
}

bool StorageChunkReader::ReadHeaderPage()
{
    if (!headerPage.Read(fd.GetFD(), readBuffer))
        return false;

    offset += headerPage.GetPageSize();
}

uint64_t StorageChunkReader::GetNumKeys()
{
    return headerPage.GetNumKeys();
}

StorageKeyValue* StorageChunkReader::First()
{
    if (!dataPage.Read(fd.GetFD(), readBuffer))
        return NULL;

    offset += dataPage.GetPageSize();

    return dataPage.First();
}

StorageKeyValue* StorageChunkReader::Next(StorageKeyValue* it)
{
    it = dataPage.Next(it);

    if (it != NULL)
        return it;
    
    if (offset >= headerPage.GetIndexOffset())
        return NULL;

    if (!dataPage.Read(fd.GetFD(), readBuffer))
        return NULL;

    offset += dataPage.GetPageSize();

    return dataPage.First();
}
