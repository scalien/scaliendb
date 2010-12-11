#include "StorageChunkFile.h"

StorageChunkFile::StorageChunkFile()
{
    dataPagesSize = 64;
    dataPages = (StorageDataPage**) malloc(sizeof(StorageDataPage*) * dataPagesSize);
    numDataPages = 0;
    fileSize = 0;
    written = false;
}

StorageChunkFile::~StorageChunkFile()
{
    free(dataPages);
}

void StorageChunkFile::SetFilename(Buffer& filename_)
{
    filename = filename_;
    filename.NullTerminate();
}

Buffer& StorageChunkFile::GetFilename()
{
    return filename;
}

bool StorageChunkFile::IsWritten()
{
    return written;
}

uint64_t StorageChunkFile::GetChunkID()
{
    return headerPage.GetChunkID();
}

bool StorageChunkFile::UseBloomFilter()
{
    return headerPage.UseBloomFilter();
}

bool StorageChunkFile::Get(ReadBuffer& key, ReadBuffer& value)
{
    uint32_t    index;
    uint32_t    offset;

    if (headerPage.UseBloomFilter())
    {
        if (!bloomPage.Check(key))
            return false;
    }

    if (!indexPage.Locate(key, index, offset))
        return false;

    return dataPages[index]->Get(key, value);
}

uint64_t StorageChunkFile::GetLogSegmentID()
{
    return headerPage.GetLogSegmentID();
}

uint32_t StorageChunkFile::GetLogCommandID()
{
    return headerPage.GetLogCommandID();
}

uint64_t StorageChunkFile::GetSize()
{
    return fileSize;
}

void StorageChunkFile::AppendDataPage(StorageDataPage* dataPage)
{
    if (numDataPages == dataPagesSize)
        ExtendDataPageArray();
    
    dataPages[numDataPages] = dataPage;
    numDataPages++;
}

void StorageChunkFile::ExtendDataPageArray()
{
    StorageDataPage**   newDataPages;
    unsigned            newSize, i;
    
    newSize = dataPagesSize * 2;
    newDataPages = (StorageDataPage**) malloc(sizeof(StorageDataPage*) * newSize);
    
    for (i = 0; i < numDataPages; i++)
        newDataPages[i] = dataPages[i];
    
    free(dataPages);
    dataPages = newDataPages;
    dataPagesSize = newSize;
}
