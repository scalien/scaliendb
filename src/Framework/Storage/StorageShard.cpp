#include "StorageShard.h"
#include "StorageFileHeader.h"
#include "System/Common.h"
#include "System/FileSystem.h"
#include "System/Buffers/Buffer.h"

#include <stdio.h>

#define RECOVERY_MARKER     0       // special as it's an illegal fileIndex
#define FILE_TYPE           "ScalienDB shard index"
#define FILE_VERSION_MAJOR  0
#define FILE_VERSION_MINOR  1
#define DATAFILE_PREFIX     "data."
#define DATAFILE_PADDING    10

static int KeyCmp(const ReadBuffer& a, const ReadBuffer& b)
{
    return ReadBuffer::Cmp(a, b);
}

static ReadBuffer& Key(StorageFileIndex* fi)
{
    return fi->key;
}

StorageShard::~StorageShard()
{
    files.DeleteTree();
}

void StorageShard::Open(const char* dir, const char* name_)
{
    int64_t recoverySize;
    int64_t tocSize;
    char    sep;
    
    nextStorageFileIndex = 1;

    // create shard directory
    if (*dir == '\0')
        path.Append(".");
    else
        path.Append(dir);

    sep = FS_Separator();
    if (path.GetBuffer()[path.GetLength() - 1] != sep)
        path.Append(&sep, 1);
    
    path.NullTerminate();
    if (!FS_IsDirectory(path.GetBuffer()))
    {
        if (!FS_CreateDir(path.GetBuffer()))
            ASSERT_FAIL();
    }

    name.Append(path.GetBuffer(), path.GetLength() - 1);
    name.Append(name_);
    name.NullTerminate();

    tocFilepath.Append(path.GetBuffer(), path.GetLength() - 1);
    tocFilepath.Append("index");
    tocFilepath.NullTerminate();

    recoveryFilepath.Append(path.GetBuffer(), path.GetLength() - 1);
    recoveryFilepath.Append("recovery");
    recoveryFilepath.NullTerminate();

    tocFD = FS_Open(tocFilepath.GetBuffer(), FS_READWRITE | FS_CREATE);
    if (tocFD == INVALID_FD)
        ASSERT_FAIL();

    recoveryFD = FS_Open(recoveryFilepath.GetBuffer(), FS_READWRITE | FS_CREATE);
    if (recoveryFD == INVALID_FD)
        ASSERT_FAIL();

    recoverySize = FS_FileSize(recoveryFD);
    if (recoverySize > 0)
    {
        PerformRecovery(recoverySize);
        return;
    }
    
    tocSize = FS_FileSize(tocFD);
    shardSize = tocSize;
    if (tocSize > 0)
        shardSize += ReadTOC(tocSize);
    prevCommitStorageFileIndex = nextStorageFileIndex;
}

void StorageShard::Commit(bool recovery, bool flush)
{
    if (recovery)
    {
        WriteRecoveryPrefix();
        // to make sure the recovery (prefix) part is written
        if (flush)
            FS_Sync();
    }

    WriteTOC();
    WriteData();
    // to make sure the data is written before we mark it such in the recovery postfix
    if (flush)
        FS_Sync();

    if (recovery)
    {
        WriteRecoveryPostfix();
        FS_FileSeek(recoveryFD, 0, FS_SEEK_SET);
        FS_FileTruncate(recoveryFD, 0);
    }
    prevCommitStorageFileIndex = nextStorageFileIndex;
}

void StorageShard::Close()
{
    StorageFileIndex*   it;
    
    for (it = files.First(); it != NULL; it = files.Next(it))
    {
        if (it->file == NULL)
            continue;
        it->file->Close();
    }
    
    files.DeleteTree();
    FS_FileClose(recoveryFD);
    FS_FileClose(tocFD);
    
    FS_Delete(recoveryFilepath.GetBuffer());
}

const char* StorageShard::GetName()
{
    return name.GetBuffer();
}

uint64_t StorageShard::GetSize()
{
    return shardSize;
}

bool StorageShard::GetMidpoint(ReadBuffer& key)
{
    StorageFileIndex* fi;
    
    fi = files.Mid();
    if (fi == NULL)
        return false;
    
    key = fi->key;
    return true;
}

ReadBuffer StorageShard::FirstKey()
{
    StorageFileIndex*   fi;
    
    fi = files.First();
    if (fi == NULL)
        return ReadBuffer();
    
    return fi->key;
}

bool StorageShard::Get(ReadBuffer& key, ReadBuffer& value)
{
    StorageFileIndex* fi;
    
    fi = Locate(key);

    if (fi == NULL)
        return false;
    
    return fi->file->Get(key, value);
}

bool StorageShard::Set(ReadBuffer& key, ReadBuffer& value, bool copy)
{
    StorageFileIndex    *fi;
    uint64_t            sizeBefore;
    
    fi = Locate(key);

    if (fi == NULL)
    {
        fi = new StorageFileIndex;
        fi->index = nextStorageFileIndex++;
        fi->file = new StorageFile;
        fi->file->SetStorageFileIndex(fi->index);
        WritePath(fi->filepath, fi->index);
        fi->file->Open(fi->filepath.GetBuffer());
        fi->SetKey(key, true); // TODO: buffer management
        files.Insert(fi);
    }
    
    sizeBefore = fi->file->GetSize();
    if (!fi->file->Set(key, value, copy))
        return false;

    shardSize -= sizeBefore;
    shardSize += fi->file->GetSize();
    // update index:
    if (ReadBuffer::LessThan(key, fi->key))
        fi->SetKey(key, true);

    if (fi->file->IsOverflowing())
        SplitFile(fi->file);
    
    return true;
}

bool StorageShard::Delete(ReadBuffer& key)
{
    bool                updateIndex;
    StorageFileIndex*   fi;
    ReadBuffer          firstKey;
    uint64_t            sizeBefore;
    uint64_t            sizeAfter;
    
    fi = Locate(key);

    if (fi == NULL)
        return false;
    
    updateIndex = false;
    firstKey = fi->file->FirstKey();
    if (BUFCMP(&key, &firstKey))
        updateIndex = true;
    
    sizeBefore = fi->file->GetSize();
    fi->file->Delete(key);
    sizeAfter = fi->file->GetSize();
    
    if (fi->file->IsEmpty())
    {
        fi->file->Close();
        FS_Delete(fi->filepath.GetBuffer());
        files.Remove(fi);
        delete fi;
        shardSize -= sizeBefore;

        return true;
    }
    else if (updateIndex)
    {
        firstKey = fi->file->FirstKey();
        fi->SetKey(firstKey, true);
    }
    
    shardSize -= (sizeAfter - sizeBefore);
    return true;
}

StorageShard* StorageShard::SplitShard(uint64_t newShardID, ReadBuffer& startKey)
{
    StorageShard*       newShard;
    StorageFileIndex*   fi;
    StorageFileIndex*   midFi;
    StorageFileIndex*   newFi;
    StorageFileIndex*   next;
    uint32_t            newIndex;
    
    newShard = new StorageShard;
    newShard->shardID = newShardID;
    newShard->shardSize = 0;
    newShard->prevCommitStorageFileIndex = 0;
    newShard->nextStorageFileIndex = 0;
    
    midFi = Locate(startKey);
    if (midFi == NULL)
        ASSERT_FAIL();
    
    newIndex = 1;
    fi = midFi;
    if (ReadBuffer::Cmp(startKey, midFi->key) > 0)
    {
        newFi = new StorageFileIndex;
        newFi->SetKey(startKey, true);
        newFi->index = newIndex++;
        newFi->file = midFi->file->SplitFileByKey(startKey);
        newShard->files.Insert(newFi);
        fi = files.Next(midFi);
    }
    
    for (/* fi is initialized earlier */; fi != NULL; fi = next)
    {
        if (ReadBuffer::Cmp(startKey, fi->key) <= 0)
        {
            next = files.Remove(fi);
            fi->index = newIndex++;
            newShard->files.Insert(fi);
        }
        else
            next = files.Next(fi);
    }
    
    return newShard;
}

void StorageShard::WritePath(Buffer& buffer, uint32_t index)
{
    return StorageShard::WritePath(buffer, path, index);
}

void StorageShard::WritePath(Buffer& buffer, Buffer& path, uint32_t index)
{
    char    buf[30];
    
    snprintf(buf, sizeof(buf), DATAFILE_PREFIX "%0*u", DATAFILE_PADDING, index);
    
    buffer.Write(path.GetBuffer(), path.GetLength() - 1);
    buffer.Append(buf);
    buffer.NullTerminate();
}

uint64_t StorageShard::ReadTOC(uint32_t length)
{
    uint32_t            i, numFiles;
    unsigned            len;
    char*               p;
    StorageFileIndex*   fi;
    int                 ret;
    uint64_t            totalSize;
    int64_t             fileSize;
    StorageFileHeader   header;
    Buffer              headerBuf;
    
    headerBuf.Allocate(STORAGEFILE_HEADER_LENGTH);
    if ((ret = FS_FileRead(tocFD, (void*) headerBuf.GetBuffer(), STORAGEFILE_HEADER_LENGTH)) < 0)
        ASSERT_FAIL();
    headerBuf.SetLength(STORAGEFILE_HEADER_LENGTH);
    if (!header.Read(headerBuf))
        ASSERT_FAIL();
    
    length -= STORAGEFILE_HEADER_LENGTH;
    buffer.Allocate(length);
    if ((ret = FS_FileRead(tocFD, (void*) buffer.GetBuffer(), length)) < 0)
        ASSERT_FAIL();
    if (ret != (int)length)
        ASSERT_FAIL();
    p = buffer.GetBuffer();
    numFiles = FromLittle32(*((uint32_t*) p));
    assert(numFiles * 8 + 4 <= length);
    p += 4;
    totalSize = 0;
    for (i = 0; i < numFiles; i++)
    {
        fi = new StorageFileIndex;
        fi->index = FromLittle32(*((uint32_t*) p));
        p += 4;
        len = FromLittle32(*((uint32_t*) p));
        p += 4;
        fi->key.SetLength(len);
        fi->key.SetBuffer(p);
        p += len;
        files.Insert(fi);
        WritePath(fi->filepath, fi->index);
        if (fi->index + 1 > nextStorageFileIndex)
            nextStorageFileIndex = fi->index + 1;

        fileSize = FS_FileSize(fi->filepath.GetBuffer());
        if (fileSize < 0)
        {
            // TODO: error handling
            ASSERT_FAIL();
        }
        totalSize += fileSize;
    }
    
    return totalSize;
}

void StorageShard::PerformRecovery(uint32_t length)
{
    char*               p;
    uint32_t            required, pageSize, marker;
    InList<Buffer>      pages;
    Buffer*             page;

    required = 0;   
    while (true)
    {
        required += 4;
        if (length < required)
            goto TruncateLog;
        if (FS_FileRead(recoveryFD, (void*) buffer.GetBuffer(), 4) < 0)
            ASSERT_FAIL();
        p = buffer.GetBuffer();
        marker = FromLittle32(*((uint32_t*) p));
        
        if (marker == RECOVERY_MARKER)
            break;

        // it's a page
        pageSize = marker;
        required += (pageSize - 4);
        if (length < required)
            goto TruncateLog;

        buffer.SetLength(4);
        buffer.Allocate(pageSize);
        // TODO: return value
        if (FS_FileRead(recoveryFD, (void*) (buffer.GetBuffer() + 4), pageSize - 4) < 0)
            ASSERT_FAIL();

        buffer.SetLength(pageSize);
        page = new Buffer;
        page->Write(buffer);
        pages.Append(page);
    }

    // first marker was hit
    // read prevCommitStorageFileIndex
    
    required += 4;  
    if (length < required)
        goto TruncateLog;

    if (FS_FileRead(recoveryFD, (void*) buffer.GetBuffer(), 4) < 0)
        ASSERT_FAIL();
    p = buffer.GetBuffer();
    prevCommitStorageFileIndex = FromLittle32(*((uint32_t*) p));

    required += 4;  
    if (length < required)
        goto TruncateLog;
    if (FS_FileRead(recoveryFD, (void*) buffer.GetBuffer(), 4) < 0)
        ASSERT_FAIL();
    p = buffer.GetBuffer();
    marker = FromLittle32(*((uint32_t*) p));
    if (marker != RECOVERY_MARKER)
        goto TruncateLog;

    // done reading prefix part

    required += 4;  
    if (length < required)
    {
        WriteBackPages(pages);
        DeleteGarbageFiles();
        RebuildTOC();
        goto TruncateLog;
    }
    if (FS_FileRead(recoveryFD, (void*) buffer.GetBuffer(), 4) < 0)
        ASSERT_FAIL();
    p = buffer.GetBuffer();
    marker = FromLittle32(*((uint32_t*) p));
    if (marker != RECOVERY_MARKER)
    {
        WriteBackPages(pages);
        DeleteGarbageFiles();
        RebuildTOC();
        goto TruncateLog;
    }

TruncateLog:
    for (page = pages.First(); page != NULL; page = pages.Delete(page));
    FS_FileSeek(recoveryFD, 0, SEEK_SET);
    FS_FileTruncate(recoveryFD, 0); 
    FS_Sync();  
    return;
}

void StorageShard::WriteBackPages(InList<Buffer>& pages)
{
    char*       p;
    int         fd;
    uint32_t    pageSize, fileIndex, offset;
    Buffer      filepath;
    Buffer*     page;
    
    for (page = pages.First(); page != NULL; page = pages.Next(page))
    {
        // parse the header part of page and write it

        p = page->GetBuffer();
        pageSize = FromLittle32(*((uint32_t*) p));
        p += 4;
        fileIndex = FromLittle32(*((uint32_t*) p));
        assert(fileIndex != 0);
        p += 4;
        offset = FromLittle32(*((uint32_t*) p));
        p += 4;
        WritePath(filepath, fileIndex);
        filepath.NullTerminate();
        fd = FS_Open(filepath.GetBuffer(), FS_READWRITE | FS_CREATE);
        FS_FileWriteOffs(fd, page->GetBuffer(), pageSize, offset);
        FS_FileClose(fd);
    }
}

void StorageShard::DeleteGarbageFiles()
{
    char*           p;
    FS_Dir          dir;
    FS_DirEntry     dirent;
    Buffer          buffer;
    Buffer          tmp;
    unsigned        len, nread;
    uint32_t        index;
    ReadBuffer      firstKey;
    
    dir = FS_OpenDir(path.GetBuffer());
    
    while ((dirent = FS_ReadDir(dir)) != FS_INVALID_DIR_ENTRY)
    {
        len = tocFilepath.GetLength() - 1;
        buffer.Write(FS_DirEntryName(dirent));
        if (buffer.GetLength() != sizeof(DATAFILE_PREFIX) - 1 + DATAFILE_PADDING)
            continue;
        if (strncmp(DATAFILE_PREFIX, buffer.GetBuffer(), sizeof(DATAFILE_PREFIX) - 1) != 0)
            continue;
        p = buffer.GetBuffer();
        len = sizeof(DATAFILE_PREFIX) - 2;
        if (p[len] != '.')
            continue;
        p += len + 1;
        len = buffer.GetLength() - (len + 1);
        index = (uint32_t) BufferToUInt64(p, len, &nread);
        if (nread != len)
            continue;

        tmp.Write(path.GetBuffer(), path.GetLength() - 1);
        tmp.Append(FS_DirEntryName(dirent));
        tmp.NullTerminate();

        if (index >= prevCommitStorageFileIndex)
            FS_Delete(tmp.GetBuffer());
    }

    FS_CloseDir(dir);
}

uint64_t StorageShard::RebuildTOC()
{
    char*               p;
    FS_Dir              dir;
    FS_DirEntry         dirent;
    Buffer              buffer;
    Buffer              tmp;
    unsigned            len, nread;
    uint32_t            index;
    StorageFileIndex*   fi;
    ReadBuffer          firstKey;
    uint64_t            totalSize;
    
    totalSize = 0;
    dir = FS_OpenDir(path.GetBuffer());
    
    while ((dirent = FS_ReadDir(dir)) != FS_INVALID_DIR_ENTRY)
    {
        buffer.Write(FS_DirEntryName(dirent));
        if (buffer.GetLength() != sizeof(DATAFILE_PREFIX) - 1 + DATAFILE_PADDING)
            continue;
        if (strncmp(DATAFILE_PREFIX, buffer.GetBuffer(), sizeof(DATAFILE_PREFIX) - 1) != 0)
            continue;
        p = buffer.GetBuffer();
        len = sizeof(DATAFILE_PREFIX) - 2;
        if (p[len] != '.')
            continue;
        p += len + 1;
        len = buffer.GetLength() - (len + 1);
        index = (uint32_t) BufferToUInt64(p, len, &nread);
        if (nread != len)
            continue;
            
        tmp.Write(path.GetBuffer(), path.GetLength() - 1);
        tmp.Append(FS_DirEntryName(dirent));
        tmp.NullTerminate();
        
        fi = new StorageFileIndex;
        fi->file = new StorageFile;
        fi->file->Open(tmp.GetBuffer());
        if (fi->file->IsEmpty())
        {
            FS_Delete(tmp.GetBuffer());
            continue;
        }
        else
            totalSize += FS_FileSize(tmp.GetBuffer());

        fi->SetKey(fi->file->FirstKey(), true);
        fi->file->Close();
        delete fi->file;
        fi->file = NULL;
        fi->index = index;

        files.Insert(fi);
    }
    
    FS_CloseDir(dir);

    WriteTOC();
    return totalSize;
}

void StorageShard::WriteRecoveryPrefix()
{
    StorageFileIndex        *it;
    uint32_t        marker = RECOVERY_MARKER;
    
    for (it = files.First(); it != NULL; it = files.Next(it))
    {
        if (it->file == NULL)
            continue;
        it->file->WriteRecovery(recoveryFD); // only dirty old data pages' buffer is written
    }
    
    if (FS_FileWrite(recoveryFD, (const void *) &marker, 4) < 0)
    {
        Log_Errno();
        ASSERT_FAIL();
    }
    
    if (FS_FileWrite(recoveryFD, (const void *) &prevCommitStorageFileIndex, 4) < 0)
    {
        Log_Errno();
        ASSERT_FAIL();
    }

    if (FS_FileWrite(recoveryFD, (const void *) &marker, 4) < 0)
    {
        Log_Errno();
        ASSERT_FAIL();
    }
}

void StorageShard::WriteRecoveryPostfix()
{
    uint32_t        marker = RECOVERY_MARKER;

    if (FS_FileWrite(recoveryFD, (const void *) &marker, 4) < 0)
        ASSERT_FAIL();
}

void StorageShard::WriteTOC()
{
    char*               p;
    uint32_t            size, len;
    uint32_t            tmp;
    StorageFileIndex    *it;
    StorageFileHeader   header;
    Buffer              writeBuf;

    FS_FileSeek(tocFD, 0, FS_SEEK_SET);
    FS_FileTruncate(tocFD, 0);
    
    header.Init(FILE_TYPE, FILE_VERSION_MAJOR, FILE_VERSION_MINOR, 0);
    header.Write(writeBuf);
    
    if (FS_FileWrite(tocFD, (const void *) writeBuf.GetBuffer(), STORAGEFILE_HEADER_LENGTH) < 0)
        ASSERT_FAIL();
    
    len = files.GetCount();
    tmp = ToLittle32(len);

    if (FS_FileWrite(tocFD, (const void *) &tmp, 4) < 0)
        ASSERT_FAIL();

    for (it = files.First(); it != NULL; it = files.Next(it))
    {       
        size = 8 + it->key.GetLength();
        writeBuf.Allocate(size);
        p = writeBuf.GetBuffer();
        *((uint32_t*) p) = ToLittle32(it->index);
        p += 4;
        len = it->key.GetLength();
        *((uint32_t*) p) = ToLittle32(len);
        p += 4;
        memcpy(p, it->key.GetBuffer(), len);
        p += len;
        if (FS_FileWrite(tocFD, (const void *) writeBuf.GetBuffer(), size) < 0)
            ASSERT_FAIL();
    }
}

void StorageShard::WriteData()
{
    StorageFileIndex        *it;
    
    for (it = files.First(); it != NULL; it = files.Next(it))
    {
        if (it->file == NULL)
            continue;
        it->file->WriteData(); // only changed data pages are written
    }
}

StorageFileIndex* StorageShard::Locate(ReadBuffer& key)
{
    StorageFileIndex*   fi;
    int         cmpres;
    
    if (files.GetCount() == 0)
        return NULL;
        
    if (ReadBuffer::LessThan(key, files.First()->key))
    {
        fi = files.First();
        goto OpenFile;
    }
    
    fi = files.Locate<ReadBuffer&>(key, cmpres);
    if (fi)
    {
        if (cmpres < 0)
            fi = files.Prev(fi);
    }
    else
        fi = files.Last();
    
OpenFile:
    if (fi->file == NULL)
    {
        fi->file = new StorageFile;
        fi->file->Open(fi->filepath.GetBuffer());
        fi->file->SetStorageFileIndex(fi->index);
    }
    
    return fi;
}

void StorageShard::SplitFile(StorageFile* file)
{
    StorageFileIndex*   newFi;
    ReadBuffer          rb;

    shardSize -= file->GetSize();

    file->ReadRest();
    newFi = new StorageFileIndex;
    newFi->file = file->SplitFile();
    newFi->index = nextStorageFileIndex++;
    newFi->file->SetStorageFileIndex(newFi->index);
    
    WritePath(newFi->filepath, newFi->index);
    newFi->file->Open(newFi->filepath.GetBuffer());

    rb = newFi->file->FirstKey();
    newFi->SetKey(rb, true); // TODO: buffer management
    files.Insert(newFi);
    
    shardSize += file->GetSize();
    shardSize += newFi->file->GetSize();
}

StorageDataPage* StorageShard::CursorBegin(StorageCursor* cursor, ReadBuffer& key)
{
    StorageFileIndex*   fi;
    StorageDataPage*    dataPage;
    
    fi = Locate(key);

    if (fi == NULL)
        return NULL;
    
    if (cursor->bulk && cursor->file != fi->file)
        fi->file->ReadRest();
    cursor->file = fi->file;
    dataPage = fi->file->CursorBegin(cursor, key);
    if (cursor->nextKey.GetLength() != 0)
        return dataPage;
    
    fi = files.Next(fi);
    if (fi != NULL)
        cursor->nextKey.Write(fi->key);

    return dataPage;
}

void StorageShard::CommitPhase1()
{
    WriteRecoveryPrefix();
}

void StorageShard::CommitPhase2()
{
    WriteTOC();
    WriteData();
}

void StorageShard::CommitPhase3()
{
    WriteRecoveryPostfix();
    
    FS_FileSeek(recoveryFD, 0, FS_SEEK_SET);
    FS_FileTruncate(recoveryFD, 0);
}

void StorageShard::CommitPhase4()
{
    prevCommitStorageFileIndex = nextStorageFileIndex;
}
