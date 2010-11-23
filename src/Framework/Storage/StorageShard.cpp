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
    
    prevCommitStorageFileIndex = 1;
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
            STOP_FAIL(1, "Cannot create directory (%s)", path.GetBuffer())
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
        STOP_FAIL(1, "Cannot open file (%s)", tocFilepath.GetBuffer())

    if (!recoveryLog.Open(recoveryFilepath.GetBuffer()))
        STOP_FAIL(1, "Cannot open file (%s)", recoveryFilepath.GetBuffer())
    
    recoverySize = recoveryLog.GetFileSize();
    if (recoverySize > 0)
        PerformRecovery(recoverySize);
    
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
        recoveryLog.Truncate(false);
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
    recoveryLog.Close();
    FS_FileClose(tocFD);
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

bool StorageShard::Append(ReadBuffer& key, ReadBuffer& value)
{
    Buffer     newValue;     
    ReadBuffer oldValue;
    ReadBuffer rbValue;

    if (Get(key, oldValue) == true)
        newValue.Append(oldValue);
    
    newValue.Append(value);

    rbValue.Wrap(newValue);

    return Set(key, rbValue);
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
        files.Remove(fi);
        deletedFiles.Insert(fi);
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
    ST_ASSERT(midFi != NULL);
    
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
    
    FS_FileSeek(tocFD, 0, FS_SEEK_SET);
    
    headerBuf.Allocate(STORAGEFILE_HEADER_LENGTH);
    FS_FileSeek(tocFD, 0, FS_SEEK_SET);
    ret = FS_FileRead(tocFD, (void*) headerBuf.GetBuffer(), STORAGEFILE_HEADER_LENGTH);
    ST_ASSERT(ret == STORAGEFILE_HEADER_LENGTH);
    headerBuf.SetLength(STORAGEFILE_HEADER_LENGTH);
    if (!header.Read(headerBuf))
        ST_ASSERT(false);
    
    length -= STORAGEFILE_HEADER_LENGTH;
    buffer.Allocate(length);
    ret = FS_FileRead(tocFD, (void*) buffer.GetBuffer(), length);
    ST_ASSERT(ret == (ssize_t) length);
    p = buffer.GetBuffer();
    numFiles = FromLittle32(*((uint32_t*) p));
    ST_ASSERT(numFiles * 8 + 4 <= length);
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
            ST_ASSERT(false);
        }
        totalSize += fileSize;
    }
    
    return totalSize;
}

void StorageShard::PerformRecovery(uint32_t length)
{
    bool    ret;
    
    recoveryCommit = false;
    
    if (!recoveryLog.Check(length))
        ST_ASSERT(false);
    
    ret = recoveryLog.PerformRecovery(MFUNC(StorageShard, OnRecoveryOp));
    if (recoveryCommit)
    {
        WriteBackPages(recoveryPages);
        DeleteGarbageFiles();
        RebuildTOC();
    }
    else
        DeleteFiles(recoveryFiles);
    
    recoveryFiles.Clear();
    recoveryPages.DeleteList();
    recoveryLog.Truncate(true);
}

void StorageShard::OnRecoveryOp()
{
    uint32_t    op;
    uint32_t    dataSize;
    uint32_t    fileIndex;
    Buffer*     buffer;
    Buffer      tmp;
    ReadBuffer  rb;
    
    op = recoveryLog.GetOp();
    dataSize = recoveryLog.GetDataSize();
    
    switch (op)
    {
    case RECOVERY_OP_DONE:
        break;
    case RECOVERY_OP_FILE:
        recoveryLog.ReadOpData(tmp);
        rb.Wrap(tmp);
        if (!rb.ReadLittle32(fileIndex))
            recoveryLog.Fail();
        else
            recoveryFiles.Append(fileIndex);
        break;
    case RECOVERY_OP_PAGE:
        buffer = new Buffer;
        buffer->Allocate(recoveryLog.GetDataSize());
        if (recoveryLog.ReadOpData(*buffer) < 0)
        {
            delete buffer;
            recoveryLog.Fail();
        }
        else
            recoveryPages.Append(buffer);
        break;
    case RECOVERY_OP_COMMIT:
        recoveryLog.ReadOpData(tmp);
        rb.Wrap(tmp);
        if (!rb.ReadLittle32(prevCommitStorageFileIndex))
            recoveryLog.Fail();
        else
            recoveryCommit = true;
        break;
    default:
        ST_ASSERT(false);
        break;
    }
}

void StorageShard::WriteBackPages(InList<Buffer>& pages)
{
    char*       p;
    FD          fd;
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
        ST_ASSERT(fileIndex != 0);
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

        if (index > prevCommitStorageFileIndex)
            FS_Delete(tmp.GetBuffer());
    }

    FS_CloseDir(dir);
}

void StorageShard::DeleteFiles(List<uint32_t>& files)
{
    uint32_t*   file;
    Buffer      tmp;
    
    FOREACH (file, files)
    {
        WritePath(tmp, *file);
        FS_Delete(tmp.GetBuffer());
    }
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
    int64_t             fileSize;
    
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

        fileSize = FS_FileSize(tmp.GetBuffer());
        if (fileSize <= 0)
            continue;
                
        fi = new StorageFileIndex;
        fi->file = new StorageFile;
        fi->file->Open(tmp.GetBuffer());
        if (fi->file->IsEmpty())
        {
//            fi->file->Close();
//            delete fi->file;
//            fi->file = NULL;
//            delete fi;
//            FS_Delete(tmp.GetBuffer());
            ST_ASSERT(false);
            continue;
        }
        else
            totalSize += fileSize;

        fi->SetKey(fi->file->FirstKey(), true);
        fi->file->Close();
        delete fi->file;
        fi->file = NULL;
        fi->index = index;
        if (fi->index + 1 > nextStorageFileIndex)
            nextStorageFileIndex = fi->index + 1;

        files.Insert(fi);
    }
    
    FS_CloseDir(dir);

    WriteTOC();

    files.DeleteTree();

    return totalSize;
}

void StorageShard::WriteRecoveryPrefix()
{
    StorageFileIndex*       it;
    Buffer                  buffer;
    
    FOREACH (it, deletedFiles)
    {
        ST_ASSERT(it->file != NULL);
        it->file->WriteRecovery(recoveryLog);
    }
    
    FOREACH (it, files)
    {
        if (it->file == NULL)
            continue;
        
        it->file->WriteRecovery(recoveryLog);
    }

    ST_ASSERT(prevCommitStorageFileIndex >= files.GetCount());
    
    buffer.AppendLittle32(prevCommitStorageFileIndex);
    if (!recoveryLog.WriteOp(RECOVERY_OP_COMMIT, sizeof(prevCommitStorageFileIndex), buffer))
        STOP_FAIL(1, "Failed writing recovery file (%s)", recoveryLog.GetFilename());
}

void StorageShard::WriteRecoveryPostfix()
{
    recoveryLog.WriteDone();
}

void StorageShard::WriteTOC()
{
    uint32_t            size;
    ssize_t             ret;
    StorageFileIndex    *it;
    StorageFileHeader   header;
    Buffer              writeBuffer;

    FS_FileSeek(tocFD, 0, FS_SEEK_SET);
    FS_FileTruncate(tocFD, 0);
    
    header.Init(FILE_TYPE, FILE_VERSION_MAJOR, FILE_VERSION_MINOR, 0);
    header.Write(writeBuffer);
    
    writeBuffer.AppendLittle32(files.GetCount());
    
    ret = FS_FileWrite(tocFD, (const void *) writeBuffer.GetBuffer(), writeBuffer.GetLength());
    ST_ASSERT(ret == (ssize_t) writeBuffer.GetLength());
    
    for (it = files.First(); it != NULL; it = files.Next(it))
    {       
        size = 8 + it->key.GetLength();
        writeBuffer.Allocate(size);
        writeBuffer.SetLength(0);
        
        writeBuffer.AppendLittle32(it->index);
        writeBuffer.AppendLittle32(it->key.GetLength());
        writeBuffer.Append(it->key);
        ret = FS_FileWrite(tocFD, (const void *) writeBuffer.GetBuffer(), size);
        ST_ASSERT(ret == (ssize_t) size);
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
    int                 cmpres;
    
    if (files.GetCount() == 0)
        return NULL;
        
    if (ReadBuffer::LessThan(key, files.First()->key))
    {
        fi = files.First();
        goto OpenFile;
    }
    
    fi = files.Locate(key, cmpres);
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
        WritePath(fi->filepath, fi->index);
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
    StorageFileIndex*   fi;
    StorageFileIndex*   next;
    
    WriteRecoveryPostfix();
    
    recoveryLog.Truncate(false);
    
    for (fi = deletedFiles.First(); fi != NULL; fi = next)
    {
        fi->file->Close();
        FS_Delete(fi->filepath.GetBuffer());
        next = deletedFiles.Remove(fi);
        delete fi;
    }
}

void StorageShard::CommitPhase4()
{
    prevCommitStorageFileIndex = nextStorageFileIndex;
}
