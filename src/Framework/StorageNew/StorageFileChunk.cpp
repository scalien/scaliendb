#include "StorageFileChunk.h"
#include "System/FileSystem.h"
#include "StoragePageCache.h"
#include "FDGuard.h"

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
    Buffer      buffer;

    if (headerPage.UseBloomFilter())
    {
        if (bloomPage.IsCached())
            StoragePageCache::RegisterHit(&bloomPage);
        if (!bloomPage.Check(key))
            return NULL;
    }

    if (indexPage.IsCached())
        StoragePageCache::RegisterHit(&indexPage);
    if (!indexPage.Locate(key, index, offset))
        return NULL;
        
    if (dataPages[index] == NULL)
    {
        // evicted, load back
        dataPages[index] = new StorageDataPage(this, index);
        if (!ReadPage(offset, buffer))
        {
            Log_Message("Unable to read page from %B at offset %U", &filename, offset);
            Log_Message("This should not happen.");
            Log_Message("Possible causes: software bug, damaged file, corrupted file...");
            Log_Message("Exiting...");
            ASSERT_FAIL();
        }
        if (!dataPages[index]->Read(buffer))
        {
            Log_Message("Unable to parse page read from %B at offset %U with size %u",
             &filename, offset, buffer.GetLength());
            Log_Message("This should not happen.");
            Log_Message("Possible causes: software bug, damaged file, corrupted file...");
            Log_Message("Exiting...");
            ASSERT_FAIL();
        }
        StoragePageCache::AddPage(dataPages[index]);
    }

    if (dataPages[index]->IsCached())
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
    
// TODO: removed for testing
//    StoragePageCache::AddPage(&indexPage);
//    if (UseBloomFilter())
//        StoragePageCache::AddPage(&bloomPage);

    for (i = 0; i < numDataPages; i++)
        StoragePageCache::AddPage(dataPages[i]);
}

void StorageFileChunk::OnPageEvicted(uint32_t index)
{
    assert(dataPages[index] != NULL);
    
    delete dataPages[index];
    dataPages[index] = NULL;
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

bool StorageFileChunk::ReadPage(uint32_t offset, Buffer& buffer)
{
    uint32_t    size, rest;
    ReadBuffer  parse;
    FDGuard     fd;
    
    if (fd.Open(filename.GetBuffer(), FS_READONLY) == INVALID_FD)
        return false;

    size = STORAGE_DEFAULT_PAGE_GRAN;
    buffer.Allocate(size);
    if (FS_FileReadOffs(fd.GetFD(), buffer.GetBuffer(), size, offset) != size)
        return false;
    buffer.SetLength(size);
        
    // first 4 bytes on all pages is the page size
    parse.Wrap(buffer);
    if (!parse.ReadLittle32(size))
        return false;
    
    rest = size - buffer.GetLength();
    if (rest > 0)
    {
        // read rest
        buffer.Allocate(size);
        if (FS_FileReadOffs(fd.GetFD(), buffer.GetPosition(), rest, offset + buffer.GetLength()) != rest)
            return false;
        buffer.SetLength(size);
    }
    
    fd.Close();
    
    return true;
}
