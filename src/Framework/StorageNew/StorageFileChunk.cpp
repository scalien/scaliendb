#include "StorageFileChunk.h"
#include "System/FileSystem.h"
#include "StoragePageCache.h"
#include "FDGuard.h"

StorageFileChunk::StorageFileChunk() : headerPage(this)
{
    prev = next = this;
    written = false;
    dataPagesSize = 64;
    dataPages = (StorageDataPage**) malloc(sizeof(StorageDataPage*) * dataPagesSize);
    memset(dataPages, 0, sizeof(StorageDataPage*) * dataPagesSize);
    indexPage = NULL;
    bloomPage = NULL;
    numDataPages = 0;
    fileSize = 0;
}

void StorageFileChunk::ReadHeaderPage()
{
    Buffer      buffer;
    uint32_t    offset;
    
    offset = 0;
    
    if (!ReadPage(offset, buffer))
    {
        Log_Message("Unable to read header page from %B at offset %U", &filename, offset);
        Log_Message("This should not happen.");
        Log_Message("Possible causes: software bug, damaged file, corrupted file...");
        Log_Message("Exiting...");
        ASSERT_FAIL();
    }
    
    if (!headerPage.Read(buffer))
    {
        Log_Message("Unable to parse header page read from %B at offset %U with size %u",
         &filename, offset, buffer.GetLength());
        Log_Message("This should not happen.");
        Log_Message("Possible causes: software bug, damaged file, corrupted file...");
        Log_Message("Exiting...");
        ASSERT_FAIL();
    }
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
        if (bloomPage == NULL)
            LoadBloomPage(); // evicted, load back
        if (bloomPage->IsCached())
            StoragePageCache::RegisterHit(bloomPage);
        if (!bloomPage->Check(key))
            return NULL;
    }

    if (indexPage == NULL)
        LoadIndexPage(); // evicted, load back
    if (indexPage->IsCached())
        StoragePageCache::RegisterHit(indexPage);
    if (!indexPage->Locate(key, index, offset))
        return NULL;
        
    if (dataPages[index] == NULL)
        LoadDataPage(index, offset); // evicted, load back

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
    uint32_t i;
    
    if (UseBloomFilter())
        StoragePageCache::AddPage(bloomPage);

    StoragePageCache::AddPage(indexPage);

    for (i = 0; i < numDataPages; i++)
        StoragePageCache::AddPage(dataPages[i]);
}

void StorageFileChunk::OnBloomPageEvicted()
{
    delete bloomPage;
    bloomPage = NULL;
}

void StorageFileChunk::OnIndexPageEvicted()
{
    delete indexPage;
    indexPage = NULL;
}

void StorageFileChunk::OnDataPageEvicted(uint32_t index)
{
    assert(dataPages[index] != NULL);
    
    delete dataPages[index];
    dataPages[index] = NULL;
}

void StorageFileChunk::LoadBloomPage()
{
    Buffer      buffer;
    uint32_t    offset;
    
    bloomPage = new StorageBloomPage(this);
    offset = headerPage.GetBloomPageOffset();
    if (!ReadPage(offset, buffer))
    {
        Log_Message("Unable to read bloom page from %B at offset %U", &filename, offset);
        Log_Message("This should not happen.");
        Log_Message("Possible causes: software bug, damaged file, corrupted file...");
        Log_Message("Exiting...");
        ASSERT_FAIL();
    }
    if (!bloomPage->Read(buffer))
    {
        Log_Message("Unable to parse bloom page read from %B at offset %U with size %u",
         &filename, offset, buffer.GetLength());
        Log_Message("This should not happen.");
        Log_Message("Possible causes: software bug, damaged file, corrupted file...");
        Log_Message("Exiting...");
        ASSERT_FAIL();
    }
    StoragePageCache::AddPage(bloomPage);
}

void StorageFileChunk::LoadIndexPage()
{
    Buffer      buffer;
    uint32_t    offset;
    
    indexPage = new StorageIndexPage(this);
    offset = headerPage.GetIndexPageOffset();
    if (!ReadPage(offset, buffer))
    {
        Log_Message("Unable to read index page from %B at offset %U", &filename, offset);
        Log_Message("This should not happen.");
        Log_Message("Possible causes: software bug, damaged file, corrupted file...");
        Log_Message("Exiting...");
        ASSERT_FAIL();
    }
    if (!indexPage->Read(buffer))
    {
        Log_Message("Unable to parse index page read from %B at offset %U with size %u",
         &filename, offset, buffer.GetLength());
        Log_Message("This should not happen.");
        Log_Message("Possible causes: software bug, damaged file, corrupted file...");
        Log_Message("Exiting...");
        ASSERT_FAIL();
    }
    StoragePageCache::AddPage(indexPage);
}

void StorageFileChunk::LoadDataPage(uint32_t index, uint32_t offset)
{
    Buffer buffer;
    
    dataPages[index] = new StorageDataPage(this, index);
    if (!ReadPage(offset, buffer))
    {
        Log_Message("Unable to read data page from %B at offset %U", &filename, offset);
        Log_Message("This should not happen.");
        Log_Message("Possible causes: software bug, damaged file, corrupted file...");
        Log_Message("Exiting...");
        ASSERT_FAIL();
    }
    if (!dataPages[index]->Read(buffer))
    {
        Log_Message("Unable to parse data page read from %B at offset %U with size %u",
         &filename, offset, buffer.GetLength());
        Log_Message("This should not happen.");
        Log_Message("Possible causes: software bug, damaged file, corrupted file...");
        Log_Message("Exiting...");
        ASSERT_FAIL();
    }
    StoragePageCache::AddPage(dataPages[index]);
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
