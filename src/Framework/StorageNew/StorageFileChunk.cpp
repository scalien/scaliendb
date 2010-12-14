#include "StorageFileChunk.h"
#include "StoragePageCache.h"

StorageFileChunk::StorageFileChunk()
: headerPage(this), indexPage(this), bloomPage(this)
{
    prev = next = this;
    written = false;
    dataPagesSize = 64;
    dataPages = (StorageDataPage**) malloc(sizeof(StorageDataPage*) * dataPagesSize);
    numDataPages = 0;
    fileSize = 0;
}

StorageFileChunk::~StorageFileChunk()
{
    free(dataPages);
}

void StorageFileChunk::SetFilename(Buffer& filename_)
{
    filename.Write(filename_);
    filename.NullTerminate();
}

Buffer& StorageFileChunk::GetFilename()
{
    return filename;
}

StorageChunk::ChunkState StorageFileChunk::GetChunkState()
{
    if (written)
        return StorageChunk::Written;
    else
        return StorageChunk::Unwritten;
}

uint64_t StorageFileChunk::GetChunkID()
{
    return headerPage.GetChunkID();
}

bool StorageFileChunk::UseBloomFilter()
{
    return headerPage.UseBloomFilter();
}

StorageKeyValue* StorageFileChunk::Get(ReadBuffer& key)
{
    uint32_t    index;
    uint32_t    offset;

    if (headerPage.UseBloomFilter())
    {
        StoragePageCache::RegisterHit(&bloomPage);
        if (!bloomPage.Check(key))
            return NULL;
    }

    StoragePageCache::RegisterHit(&indexPage);
    if (!indexPage.Locate(key, index, offset))
        return NULL;

    StoragePageCache::RegisterHit(dataPages[index]);
    return dataPages[index]->Get(key);
}

uint64_t StorageFileChunk::GetLogSegmentID()
{
    return headerPage.GetLogSegmentID();
}

uint32_t StorageFileChunk::GetLogCommandID()
{
    return headerPage.GetLogCommandID();
}

uint64_t StorageFileChunk::GetSize()
{
    return fileSize;
}

void StorageFileChunk::AddPagesToCache()
{
    unsigned i;
    
    StoragePageCache::AddPage(&indexPage);

    if (UseBloomFilter())
        StoragePageCache::AddPage(&bloomPage);

    for (i = 0; i < numDataPages; i++)
        StoragePageCache::AddPage(dataPages[i]);
}

void StorageFileChunk::AppendDataPage(StorageDataPage* dataPage)
{
    if (numDataPages == dataPagesSize)
        ExtendDataPageArray();
    
    dataPages[numDataPages] = dataPage;
    numDataPages++;
}

void StorageFileChunk::ExtendDataPageArray()
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
