#include "StorageFile.h"
#include "StorageFileHeader.h"
#include "StorageDataCache.h"
#include "StorageCursor.h"
#include "StorageRecoveryLog.h"
#include "StorageDefaults.h"
#include "System/FileSystem.h"
#include "stdio.h"

#define INDEXPAGE_OFFSET    (STORAGEFILE_HEADER_LENGTH+INDEXPAGE_HEADER_SIZE)

#define DATAPAGE_OFFSET(idx) (INDEXPAGE_OFFSET+indexPageSize+(idx)*dataPageSize)
#define DATAPAGE_INDEX(offs) (((offs)-INDEXPAGE_OFFSET-indexPageSize)/dataPageSize)
#define FILE_TYPE           "ScalienDB data file"
#define FILE_VERSION_MAJOR  0
#define FILE_VERSION_MINOR  1

#define ST_FIRSTKEY_ASSERT(expr) \
{ \
    if (!(expr)) \
    { \
        indexPage.DumpKeys(); \
        DumpKeys(dataPages); \
        ST_ASSERT(expr); \
    } \
}


static void DumpKeys(StorageDataPage** dataPages)
{
    ReadBuffer  firstKey;
    
    for (int i = 0; i < 256; i++)
    {
        if (dataPages[i] == NULL)
            printf("StorageData: %i: NULL\n", i);
        else
        {
            firstKey = dataPages[i]->FirstKey();
            printf("StorageData: %i: %.*s\n", i, P(&firstKey));
        }
    }
}

static int KeyCmp(const uint32_t a, const uint32_t b)
{
    if (a < b)
        return -1;
    else if (a > b)
        return 1;
    else
        return 0;       
}

static uint32_t Key(StoragePage* page)
{
    return page->GetOffset();
}

StorageFile::StorageFile()
{
    uint32_t i;
        
    indexPageSize = DEFAULT_INDEXPAGE_SIZE;
    dataPageSize = DEFAULT_DATAPAGE_SIZE;
    numDataPageSlots = DEFAULT_NUM_DATAPAGES;

    isOverflowing = false;
    isDeleted = false;
    newFile = true;
    
    indexPage.SetOffset(INDEXPAGE_OFFSET);
    indexPage.SetPageSize(indexPageSize);
    indexPage.SetNumDataPageSlots(numDataPageSlots);

    dataPages = (StorageDataPage**) malloc(sizeof(StorageDataPage*) * numDataPageSlots);
    for (i = 0; i < numDataPageSlots; i++)
        dataPages[i] = NULL;
    numDataPages = 0;
    
    fd = INVALID_FD;
}

StorageFile::~StorageFile()
{
    // dirtyPages' destructor automatically calls this
    // but it is only running after all pages are free'd.
    dirtyPages.Clear();
    
    for (uint32_t u = 0; u < numDataPages; u++)
    {
        if (dataPages[u] != NULL)
        {
            if (!dataPages[u]->IsDirty())
                DCACHE->Checkout(dataPages[u]);

            DCACHE->FreePage(dataPages[u]);
        }
    }
    free(dataPages);
}

void StorageFile::Open(const char* filepath_)
{
    filepath.Write(filepath_);
    filepath.NullTerminate();

    fd = FS_Open(filepath.GetBuffer(), FS_READWRITE | FS_CREATE);

    if (fd == INVALID_FD)
        ASSERT_FAIL();

    if (FS_FileSize(fd) > 0)
        Read();
}

void StorageFile::Close()
{
    FS_FileClose(fd);
    
    fd = INVALID_FD;
}

void StorageFile::SetStorageFileIndex(uint32_t fileIndex_)
{
    StoragePage*    it;
    
    fileIndex = fileIndex_;
    
    indexPage.SetStorageFileIndex(fileIndex);
    for (it = dirtyPages.First(); it != NULL; it = dirtyPages.Next(it))
        it->SetStorageFileIndex(fileIndex);
}

bool StorageFile::Get(ReadBuffer& key, ReadBuffer& value)
{
    int32_t index;
    bool    ret;
    
    index = Locate(key);
    
    if (index < 0)
        return false;
    
    ret = dataPages[index]->Get(key, value);
    if (ret && !dataPages[index]->IsDirty())
        DCACHE->RegisterHit(dataPages[index]);

    return ret;
}

bool StorageFile::Set(ReadBuffer& key, ReadBuffer& value, bool copy)
{
    int32_t             index;
    ReadBuffer          rb;
    StorageDataPage*    dataPage;
    
    if (key.GetLength() + value.GetLength() > DATAPAGE_MAX_KV_SIZE(dataPageSize))
        return false;
    
    index = Locate(key);
    if (index >= 0)
        ST_FIRSTKEY_ASSERT(indexPage.IsKey(index, dataPages[index]->FirstKey()) == true);
    
    if (index < 0)
    {
        // file is empty, allocate first data page
        index = 0;
        ST_ASSERT(numDataPages == 0);
        ST_ASSERT(dataPages[index] == NULL);
        dataPage = dataPages[index] = DCACHE->GetPage();
        DCACHE->Checkin(dataPage);
        dataPage->SetStorageFileIndex(fileIndex);
        dataPage->SetOffset(DATAPAGE_OFFSET(index));
        dataPage->SetPageSize(dataPageSize);
        dataPage->SetFile(this);
        ST_ASSERT(numDataPages == numDataPageSlots - indexPage.freeDataPages.GetLength());
        numDataPages++;
        indexPage.Add(key, index, true); // TODO
        MarkPageDirty(&indexPage);
    }
    else
    {
        dataPage = dataPages[index];
        rb = dataPage->FirstKey();
        if (ReadBuffer::LessThan(key, rb))
        {
            STORAGE_TRACE("Changing index entry for %u to %.*s", index, P(&key));
            indexPage.Update(key, index, true);
            MarkPageDirty(&indexPage);
        }
    }
    
    if (dataPage->HasCursors())
    {
        // no cache because cursor has the detached copy
        dataPage->Detach();
        ST_ASSERT(dataPage->HasCursors() == false);
    }
    
    if (!dataPage->Set(key, value, copy))
    {
        if (!dataPage->IsDirty())
            DCACHE->RegisterHit(dataPage);

        ST_FIRSTKEY_ASSERT(indexPage.IsKey(index, dataPages[index]->FirstKey()) == true);
        return true; // nothing changed
    }
    
    MarkPageDirty(dataPage);
    
    if (dataPage->IsOverflowing())
        SplitDataPage(index);
    
    ST_FIRSTKEY_ASSERT(indexPage.IsKey(index, dataPages[index]->FirstKey()) == true);
    return true;
}

void StorageFile::Delete(ReadBuffer& key)
{
    bool        updateIndex;
    int32_t     index;
    ReadBuffer  firstKey;
    
    index = Locate(key);
    
    if (index < 0)
        return;

    ST_FIRSTKEY_ASSERT(indexPage.IsKey(index, dataPages[index]->FirstKey()) == true);

    updateIndex = false;
    firstKey = dataPages[index]->FirstKey();
    if (BUFCMP(&key, &firstKey))
        updateIndex = true;

    if (dataPages[index]->HasCursors())
    {
        dataPages[index]->Detach();
        ST_ASSERT(dataPages[index]->HasCursors() == false);
    }

    dataPages[index]->Delete(key);
    MarkPageDirty(dataPages[index]);

    if (dataPages[index]->IsEmpty())
    {
        indexPage.Remove(key);
        MarkPageDirty(&indexPage);
        numDataPages--;
        dataPages[index]->SetDeleted(true);
        dataPages[index] = NULL;
        STORAGE_TRACE("Delete empty");
    }
    else if (updateIndex)
    {
        firstKey = dataPages[index]->FirstKey();
        indexPage.Update(firstKey, index, true);
        MarkPageDirty(&indexPage);
        STORAGE_TRACE("Delete updateIndex");
    }
    else
        STORAGE_TRACE("Delete else");
    ST_FIRSTKEY_ASSERT(indexPage.IsKey(index, dataPages[index]->FirstKey()) == true);
}

ReadBuffer StorageFile::FirstKey()
{
    return indexPage.FirstKey();
}

bool StorageFile::IsEmpty()
{
    return indexPage.IsEmpty();
}

bool StorageFile::IsNew()
{
    return newFile;
}

bool StorageFile::IsOverflowing()
{
    return isOverflowing || indexPage.IsOverflowing();
}

StorageFile* StorageFile::SplitFile()
{
    StorageFile*    newFile;
    uint32_t        index, newIndex, num;
    
    ST_ASSERT(numDataPageSlots == numDataPages);
    
    // TODO: do thing for cursors
    
    newFile = new StorageFile;
    newFile->indexPageSize = indexPageSize;
    newFile->dataPageSize = dataPageSize;
    newFile->numDataPageSlots = numDataPageSlots;
    newFile->indexPage.SetPageSize(indexPageSize);
    newFile->indexPage.SetNumDataPageSlots(numDataPageSlots);
    
    ReorderPages();
    
    num = numDataPages;
    for (index = numDataPageSlots / 2, newIndex = 0; index < num; index++, newIndex++)
    {
        ST_ASSERT(dataPages[index]->IsEmpty() != true);
        newFile->dataPages[newIndex] = dataPages[index];
        dataPages[index] = NULL;
        newFile->dataPages[newIndex]->SetOffset(DATAPAGE_OFFSET(newIndex));
        newFile->dataPages[newIndex]->SetFile(newFile);

        ST_ASSERT(numDataPages == numDataPageSlots - indexPage.freeDataPages.GetLength());
        ST_ASSERT(newFile->numDataPages == newFile->numDataPageSlots - newFile->indexPage.freeDataPages.GetLength());
        numDataPages--;
        newFile->numDataPages++;
        ST_ASSERT(newFile->numDataPages < newFile->numDataPageSlots);

        indexPage.Remove(newFile->dataPages[newIndex]->FirstKey());
        newFile->indexPage.Add(newFile->dataPages[newIndex]->FirstKey(), newIndex, true);
    }
    
    isOverflowing = false;
    for (index = 0; index < numDataPages; index++)
    {
        if (dataPages[index]->IsOverflowing())
            SplitDataPage(index);
    }
    ST_ASSERT(isOverflowing == false);
    
    newFile->isOverflowing = false;
    for (index = 0; index < newFile->numDataPages; index++)
    {
        if (newFile->dataPages[index]->IsOverflowing())
            newFile->SplitDataPage(index);
    }
    ST_ASSERT(newFile->isOverflowing == false);
    
    ReorderFile();
    newFile->ReorderFile();
        
    return newFile;
}

StorageFile* StorageFile::SplitFileByKey(ReadBuffer& startKey)
{
    StorageDataPage*    page;
    StorageDataPage*    newPage;
    StorageFile*        newFile;
    uint32_t            index;
    uint32_t            splitIndex;
    int32_t             ret;
    int                 cmp;
    
    newFile = new StorageFile;
    newFile->indexPageSize = indexPageSize;
    newFile->dataPageSize = dataPageSize;
    newFile->numDataPageSlots = numDataPageSlots;
    newFile->indexPage.SetPageSize(indexPageSize);
    newFile->indexPage.SetNumDataPageSlots(numDataPageSlots);

    ReadRest();
    ReorderPages();
    
    ret = Locate(startKey);
    ST_ASSERT(ret >= 0);
    splitIndex = (uint32_t) ret;
    
    for (index = 0; index < numDataPageSlots; index++)
    {
        page = dataPages[index];
        if (page == NULL)
            break;          // the pages are already ordered
    
        cmp = ReadBuffer::Cmp(startKey, page->FirstKey());
        if (cmp <= 0)
        {
            if (cmp < 0 && index == splitIndex)
            {
                // startKey is not the first key of the page so we have to split it
                newPage = page->SplitDataPageByKey(startKey);
                newFile->dataPages[index] = newPage;
            }
            else
            {
                ST_ASSERT(numDataPages == numDataPageSlots - indexPage.freeDataPages.GetLength());
                newFile->dataPages[index] = dataPages[index];
                indexPage.Remove(dataPages[index]->FirstKey()); 
                dataPages[index]->SetDeleted(true);
                dataPages[index] = NULL;
                numDataPages--;
            }

            ST_ASSERT(newFile->numDataPages == newFile->numDataPageSlots - newFile->indexPage.freeDataPages.GetLength());
            newFile->dataPages[index]->SetOffset(DATAPAGE_OFFSET(index));
            newFile->dataPages[index]->SetFile(newFile);
            newFile->dataPages[index]->Invalidate();
            MarkPageDirty(newFile->dataPages[index]);
            newFile->indexPage.Add(newFile->dataPages[index]->FirstKey(), index, true);     
            newFile->numDataPages++;
        }
    }
    
    ReorderFile();
    newFile->ReorderFile();
    
    return newFile;
}

void StorageFile::Read()
{
    unsigned            i;
    int                 length;
    Buffer              buffer;
    ReadBuffer          readBuffer;
    StorageFileHeader   header;
    bool                ret;
    
    newFile = false;

    // read file header
    buffer.Allocate(STORAGEFILE_HEADER_LENGTH);
    if (FS_FileRead(fd, (void*) buffer.GetBuffer(), STORAGEFILE_HEADER_LENGTH) != STORAGEFILE_HEADER_LENGTH)
        ST_ASSERT(false);
    buffer.SetLength(STORAGEFILE_HEADER_LENGTH);
    if (!header.Read(buffer))
        ST_ASSERT(false);
    
    // read index header
    buffer.Allocate(INDEXPAGE_HEADER_SIZE);
    if (FS_FileRead(fd, (void*) buffer.GetBuffer(), INDEXPAGE_HEADER_SIZE) != INDEXPAGE_HEADER_SIZE)
        ST_ASSERT(false);
    readBuffer.Wrap(buffer);
    ret = readBuffer.ReadLittle32(indexPageSize);
    ST_ASSERT(ret == true);
    ST_ASSERT(indexPageSize != 0);
    
    readBuffer.Advance(sizeof(uint32_t));
    ret = readBuffer.ReadLittle32(dataPageSize);
    ST_ASSERT(ret == true);
    ST_ASSERT(dataPageSize != 0);

    readBuffer.Advance(sizeof(uint32_t));
    ret = readBuffer.ReadLittle32(numDataPageSlots);
    ST_ASSERT(ret == true);
    ST_ASSERT(numDataPageSlots != 0);

    indexPage.SetOffset(INDEXPAGE_OFFSET);
    indexPage.SetPageSize(indexPageSize);
    indexPage.SetNew(false);
    indexPage.SetNumDataPageSlots(numDataPageSlots);
    
    // read index page
    buffer.Allocate(indexPageSize);
    length = FS_FileRead(fd, (void*) buffer.GetBuffer(), indexPageSize);
    if (length != (int) indexPageSize)
        ST_ASSERT(false);
    buffer.SetLength(length);
    readBuffer.Wrap(buffer);
    indexPage.Read(readBuffer);

    // TODO: this is expensive, remove this later
    ST_ASSERT(GetSize() <= (uint64_t) FS_FileSize(fd));
    
    // allocate memory for data page slots
    if (dataPages != NULL)
        free(dataPages);
    dataPages = (StorageDataPage**) malloc(sizeof(StorageDataPage*) * numDataPageSlots);
    for (i = 0; i < numDataPageSlots; i++)
        dataPages[i] = NULL;
    numDataPages = indexPage.NumEntries();  
    
    ST_ASSERT(numDataPages == numDataPageSlots - indexPage.freeDataPages.GetLength());
}

void StorageFile::ReadRest()
{
    StorageKeyIndex*        it;
    uint32_t*               uit;
    SortedList<uint32_t>    indexes;

    // As IO occurs in order, sort by index rather than by name
    for (it = indexPage.keys.First(); it != NULL; it = indexPage.keys.Next(it))
    {
        if (dataPages[it->index] == NULL)
            indexes.Add(it->index);
    }

    for (uit = indexes.First(); uit != NULL; uit = indexes.Next(uit))
        LoadDataPage(*uit);
}

void StorageFile::WriteRecovery(StorageRecoveryLog& recoveryLog)
{
    Buffer          buffer;
    StoragePage*    it;

    if (IsNew())
    {
        buffer.AppendLittle32(fileIndex);
        if (!recoveryLog.WriteOp(RECOVERY_OP_FILE, sizeof(fileIndex), buffer))
        {
            Log_Errno();
            ST_ASSERT(false);
        }
        return;
    }

    for (it = dirtyPages.First(); it != NULL; it = dirtyPages.Next(it))
    {
        if (it->IsNew())
            continue;
        ST_ASSERT(it->buffer.GetLength() <= it->GetPageSize());
        // it->buffer contains the old page
        buffer.Allocate(it->GetPageSize());
        if (!it->CheckWrite(buffer))
            continue;
        if (!recoveryLog.WriteOp(RECOVERY_OP_PAGE, it->GetPageSize(), it->buffer))
        {
            Log_Errno();
            ST_ASSERT(false);
        }
    }
}

void StorageFile::WriteData()
{
    Buffer                  buffer;
    StoragePage*            it;
    InList<StoragePage>     dirties;
    StoragePage*            next;
    StorageFileHeader       header;
    ssize_t                 ret;
    
    if (newFile)
    {
        buffer.Allocate(STORAGEFILE_HEADER_LENGTH);
        header.Init(FILE_TYPE, FILE_VERSION_MAJOR, FILE_VERSION_MINOR, 0);
        header.Write(buffer);
        if (FS_FileWrite(fd, (const void *) buffer.GetBuffer(), STORAGEFILE_HEADER_LENGTH) != STORAGEFILE_HEADER_LENGTH)
            ST_ASSERT(false);
        
        buffer.Allocate(INDEXPAGE_HEADER_SIZE);
        buffer.SetLength(0);
        
        buffer.AppendLittle32(indexPageSize);
        buffer.AppendLittle32(dataPageSize);
        buffer.AppendLittle32(numDataPageSlots);

        ST_ASSERT(buffer.GetLength() == INDEXPAGE_HEADER_SIZE);
        
        if (FS_FileWrite(fd, (const void *) buffer.GetBuffer(), INDEXPAGE_HEADER_SIZE) != INDEXPAGE_HEADER_SIZE)
            ST_ASSERT(false);
        
        newFile = false;
    }
    
    for (it = dirtyPages.First(); it != NULL; it = dirtyPages.Remove(it))
    {
        buffer.Allocate(it->GetPageSize());
        buffer.Zero();
        buffer.SetLength(0);
        //STORAGE_TRACE("writing file %s at offset %u", filepath.GetBuffer(), it->GetOffset());
        if (it->Write(buffer))
        {
            ret = FS_FileWriteOffs(fd, buffer.GetBuffer(), it->GetPageSize(), it->GetOffset());
            if (ret != (int) it->GetPageSize())
                ST_ASSERT(false);
        }
        it->SetDirty(false);
        it->SetNew(false);

        dirties.Append(it);
    }
    
    for (it = dirties.First(); it != NULL; it = next)
    {
        next = dirties.Remove(it);
        if (it->GetType() == STORAGE_DATA_PAGE)
        {
            if (it->IsDeleted())
                DCACHE->FreePage((StorageDataPage*) it);
            else
                DCACHE->Checkin((StorageDataPage*) it);
        }
    }

    // truncate back
    if (numDataPages != numDataPageSlots)
    {
        if (fd != INVALID_FD && !IsNew())
            FS_FileTruncate(fd, DATAPAGE_OFFSET(numDataPages));
    }
    
    // TODO: this is expensive, remove later
    ST_ASSERT(GetSize() == (uint64_t) FS_FileSize(fd));
}

StorageDataPage* StorageFile::CursorBegin(StorageCursor* cursor, ReadBuffer& key)
{
    int32_t index;
    
    index = indexPage.Locate(key, &cursor->nextKey);
    
    if (index < 0)  // file is empty
        return NULL;
    
    if (dataPages[index] == NULL)
        LoadDataPage(index);
    
    return dataPages[index];
}

uint64_t StorageFile::GetSize()
{
    uint64_t    size;
    
    size = STORAGEFILE_HEADER_LENGTH + INDEXPAGE_HEADER_SIZE + indexPageSize +
            (indexPage.GetMaxDataPageIndex() + 1) * dataPageSize;

    return size;
}

int32_t StorageFile::Locate(ReadBuffer& key)
{
    int32_t index;
    
    index = indexPage.Locate(key);
    
    if (index < 0)  // file is empty
        return index;
    
    if (dataPages[index] == NULL)
        LoadDataPage(index);
    
    return index;
}

void StorageFile::LoadDataPage(uint32_t index)
{
    Buffer      buffer;
    ReadBuffer  readBuffer;
    int         length;
    
    // load existing data page from disk
    dataPages[index] = DCACHE->GetPage();
    DCACHE->Checkin(dataPages[index]);

    dataPages[index]->SetOffset(DATAPAGE_OFFSET(index));
    dataPages[index]->SetPageSize(dataPageSize);
    dataPages[index]->SetNew(false);
    dataPages[index]->SetFile(this);
        
    STORAGE_TRACE("loading data page from %s at index %u", filepath.GetBuffer(), index);

    buffer.Allocate(dataPageSize);
    STORAGE_TRACE("reading page %u from %u", index, DATAPAGE_OFFSET(index));
    length = FS_FileReadOffs(fd, buffer.GetBuffer(), dataPageSize, DATAPAGE_OFFSET(index));
    ST_ASSERT(length == (int) dataPageSize);
    
    buffer.SetLength(length);
    readBuffer.Wrap(buffer);
    dataPages[index]->Read(readBuffer);
    ST_FIRSTKEY_ASSERT(indexPage.IsKey(index, dataPages[index]->FirstKey()) == true);
}

// this is called by DCACHE->FreePage
void StorageFile::UnloadDataPage(StorageDataPage* page)
{
    int32_t index;
    
    index = DATAPAGE_INDEX(page->GetOffset());
    ST_ASSERT(dataPages[index] == page);
    dataPages[index] = NULL;
}

void StorageFile::MarkPageDirty(StoragePage* page)
{
    if (!page->IsDirty())
    {
        if (page->GetType() == STORAGE_DATA_PAGE)
            DCACHE->Checkout((StorageDataPage*) page);
        
        page->SetDirty(true);
        dirtyPages.Insert(page);
    }
}

void StorageFile::SplitDataPage(uint32_t index)
{
    uint32_t            newIndex;
    StorageDataPage*    newPage;

    ST_ASSERT(numDataPages == numDataPageSlots - indexPage.freeDataPages.GetLength());
    if (numDataPages < numDataPageSlots)
    {
        // make a copy of data
        newPage = dataPages[index]->SplitDataPage();
        newPage->SetStorageFileIndex(fileIndex);
        newPage->SetFile(this);
        numDataPages++;
        ST_ASSERT(numDataPages <= numDataPageSlots);
        newIndex = indexPage.NextFreeDataPage();
        newPage->SetOffset(DATAPAGE_OFFSET(newIndex));
        ST_ASSERT(dataPages[newIndex] == NULL);
        dataPages[newIndex] = newPage;
        indexPage.Add(newPage->FirstKey(), newIndex, true); // TODO
        ST_ASSERT(dataPages[index]->IsDirty() == true);
        ST_ASSERT(newPage->IsDirty() == false);
        MarkPageDirty(newPage);
        MarkPageDirty(&indexPage);

        ST_FIRSTKEY_ASSERT(indexPage.IsKey(newIndex, dataPages[newIndex]->FirstKey()) == true);
        
        // if the newly splitted page does not fit to a data page, split it again
        if (newPage->IsOverflowing())
            SplitDataPage(newIndex);
    }
    else
        isOverflowing = true;
}

void StorageFile::ReorderPages()
{
    uint32_t            newIndex, oldIndex, i;
    StorageKeyIndex*    it;
    StorageDataPage**   newDataPages;
    
    newDataPages = (StorageDataPage**) calloc(numDataPageSlots, sizeof(StorageDataPage*));
    
    for (it = indexPage.keys.First(), newIndex = 0; it != NULL; it = indexPage.keys.Next(it), newIndex++)
    {
        ST_ASSERT(newIndex < numDataPageSlots);
        oldIndex = it->index;
        ST_ASSERT(dataPages[oldIndex] != NULL);
        it->index = newIndex;
        newDataPages[newIndex] = dataPages[oldIndex];
        newDataPages[newIndex]->SetOffset(DATAPAGE_OFFSET(newIndex));
    }
    
    free(dataPages);
    dataPages = newDataPages;
    
    indexPage.freeDataPages.Clear();
    for (i = numDataPages; i < numDataPageSlots; i++)
        indexPage.freeDataPages.Add(i);
}

void StorageFile::ReorderFile()
{
    uint32_t index;
    
    dirtyPages.Clear();
    ReorderPages();
    indexPage.SetDirty(false);
    MarkPageDirty(&indexPage);
    for (index = 0; index < numDataPages; index++)
    {
        // HACK: if it was dirty, it must be checked in before marked again as dirty
        if (dataPages[index]->IsDirty())
        {
            dataPages[index]->SetDirty(false);
            DCACHE->Checkin(dataPages[index]);
        }
        else
            dataPages[index]->SetDirty(false);

        MarkPageDirty(dataPages[index]);
    }
}

