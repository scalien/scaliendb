#include "StorageTable.h"
#include "StorageFileHeader.h"
#include "System/FileSystem.h"

#include <ctype.h>

#define LAST_SHARD_KEY      "\377\377\377\377\377\377\377\377"
#define FILE_TYPE           "ScalienDB table index"
#define FILE_VERSION_MAJOR  0
#define FILE_VERSION_MINOR  1

#define RECOVERY_OP_DONE    0
#define RECOVERY_OP_CREATE  1
#define RECOVERY_OP_COPY    2
#define RECOVERY_OP_MOVE    3

static int KeyCmp(const ReadBuffer& a, const ReadBuffer& b)
{
    return ReadBuffer::Cmp(a, b);
}

static const ReadBuffer& Key(StorageShardIndex* si)
{
    return si->startKey;
}

const char* StorageTable::GetName()
{
    return name.GetBuffer();
}

uint64_t StorageTable::GetSize()
{
    StorageShardIndex*  si;
    uint64_t            size;
    
    size = 0;
    for (si = shards.First(); si != NULL; si = shards.Next(si))
        size += si->shard->GetSize();
    
    return size;
}

void StorageTable::Open(const char* dir, const char* name_)
{
    int64_t recoverySize;
    int64_t tocSize;
    char    sep;
    
    // create table directory
    if (*dir == '\0')
        path.Append(".");
    else
        path.Append(dir);

    sep = FS_Separator();
    if (path.GetBuffer()[path.GetLength() - 1] != sep)
        path.Append(&sep, 1);
    
    path.Append(name_);
    path.Append(&sep, 1);
    path.NullTerminate();
    if (!FS_IsDirectory(path.GetBuffer()))
    {
        if (!FS_CreateDir(path.GetBuffer()))
            ASSERT_FAIL();
    }
    
    name.Write(name_);
    name.NullTerminate();

    tocFilepath.Write(path.GetBuffer(), path.GetLength() - 1);
    tocFilepath.Append("shards");
    tocFilepath.NullTerminate();

    recoveryFilepath.Write(path.GetBuffer(), path.GetLength() - 1);
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
    if (tocSize > 0)
        ReadTOC(tocSize);

    // TODO: make this controllable
    // create default shard if not exists
    if (shards.GetCount() == 0)
    {
        ReadBuffer  startKey;
        
        CreateShard(0, startKey);
    }

}

void StorageTable::Commit(bool recovery, bool flush)
{
    if (recovery)
    {
        CommitPhase1();
        if (flush)
            FS_Sync();
    }

    CommitPhase2();
    if (flush)
        FS_Sync();

    if (recovery)
        CommitPhase3();
    
    CommitPhase4();
}

void StorageTable::Close()
{
    StorageShardIndex*  it;
    
    for (it = shards.First(); it != NULL; it = shards.Next(it))
    {
        if (it->shard == NULL)
            continue;
        it->shard->Close();
    }
    
    shards.DeleteTree();

    FS_FileClose(tocFD);
    tocFD = INVALID_FD;
}

bool StorageTable::Get(ReadBuffer key, ReadBuffer& value)
{
    StorageShardIndex* si;
    
    si = Locate(key);

    if (si == NULL)
        return false;
    
    return si->shard->Get(key, value);
}

bool StorageTable::Set(ReadBuffer key, ReadBuffer value, bool copy)
{
    StorageShardIndex   *si;
    
    si = Locate(key);

    if (si == NULL)
        return false;
    
    return si->shard->Set(key, value, copy);
}

bool StorageTable::Delete(ReadBuffer key)
{
    StorageShardIndex*  si;
    ReadBuffer          firstKey;
    
    si = Locate(key);

    if (si == NULL)
        return false;
    
    return si->shard->Delete(key);
}

bool StorageTable::CreateShard(uint64_t shardID, ReadBuffer& startKey_)
{
    StorageShardIndex*  si;
    
    // TODO: check if the shard or the key interval already exists
    si = new StorageShardIndex;
    si->SetStartKey(startKey_, true);
    si->shardID = shardID;
    shards.Insert(si);
    
    return true;
}

bool StorageTable::SplitShard(uint64_t oldShardID, uint64_t newShardID, ReadBuffer& startKey)
{
    StorageShardIndex*  si;
    StorageShardIndex*  newSi;
    StorageFileIndex*   fi;
    Buffer              srcFile;
    Buffer              dstFile;
    Buffer              dirName;
    Buffer              tmp;
    int32_t             oldFileIndex;
        
    // write all changes to disk first
    Commit();
    
    dirName.Write(path.GetBuffer(), path.GetLength() - 1);
    dirName.Appendf("%U", newShardID);
    dirName.NullTerminate();

    si = Locate(startKey);
    assert(si != NULL);
    assert(si->shardID == oldShardID);

    WriteRecoveryCreateShard(oldShardID, newShardID);
    
    newSi = new StorageShardIndex;
    newSi->shard = si->shard->SplitShard(newShardID, startKey);
    newSi->shard->Open(dirName.GetBuffer(), name.GetBuffer());
    newSi->shardID = newShardID;
    newSi->SetStartKey(startKey, true);
    shards.Insert(newSi);
    
    for (fi = newSi->shard->files.First(); fi != NULL; fi = newSi->shard->files.Next(fi))
    {
        if (fi->file != NULL && fi->file->IsNew())
        {
            // TODO: HACK
            oldFileIndex = si->shard->Locate(startKey)->index;
            WriteRecoveryCopy(oldShardID, oldFileIndex);
            
            newSi->shard->WritePath(fi->filepath, fi->index);
            fi->file->Open(fi->filepath.GetBuffer());
            fi->file->WriteData();
        }
        else
        {
            srcFile.Write(fi->filepath);
            newSi->shard->WritePath(dstFile, fi->index);
            fi->filepath.Write(srcFile);
            // TODO: HACK it would be better to store shardIDs here instead of paths
            WriteRecoveryMove(srcFile, dstFile);
            if (!FS_Rename(srcFile.GetBuffer(), dstFile.GetBuffer()))
                ASSERT_FAIL();
        }
    }

    newSi->shard->WriteTOC();
    si->shard->WriteTOC();
    
    WriteTOC();
    
    WriteRecoveryDone();
    
    return true;
}

bool StorageTable::ShardExists(ReadBuffer& startKey)
{
    StorageShardIndex* si;
    
    si = Locate(startKey);
    if (!si)
        return false;
    
    if (ReadBuffer::Cmp(si->startKey, startKey) != 0)
        return false;
    
    return true;
}

StorageShardIndex* StorageTable::Locate(ReadBuffer& key)
{
    StorageShardIndex*  si;
    int                 cmpres;
    
    if (shards.GetCount() == 0)
        return NULL;
    
    if (ReadBuffer::LessThan(key, shards.First()->startKey))
    {
        ASSERT_FAIL();
        return NULL;
    }
        
    si = shards.Locate<ReadBuffer&>(key, cmpres);
    if (si)
    {
        if (cmpres < 0)
            si = shards.Prev(si);
    }
    else
        si = shards.Last();
    
    if (si->shard == NULL)
    {
        Buffer  dir;
        
        dir.Write(path.GetBuffer(), path.GetLength() - 1);
        dir.Appendf("%U", si->shardID);
        dir.NullTerminate();
        
        si->shard = new StorageShard;
        si->shard->Open(dir.GetBuffer(), name.GetBuffer());
    }
    
    return si;
}

void StorageTable::PerformRecovery(uint64_t length)
{
    char*               p;
    int32_t             required;
    uint32_t            op;
    uint64_t            total;
    uint64_t            pos;
    uint64_t            oldShardID;
    uint64_t            newShardID;
    bool                recovery;
    
    oldShardID = 0;
    newShardID = 0;
    pos = 0;
    recovery = false;

    // first check if the recovery file is written totally and is correct
    while (true)
    {
        required = sizeof(op) + sizeof(total);
        if (FS_FileRead(recoveryFD, (void*) buffer.GetBuffer(), required) != required)
            break;

        p = buffer.GetBuffer();
        op = FromLittle32(*((uint32_t*) p));
        p += sizeof(op);
        total = FromLittle64(*((uint64_t*) p));
        if (op > RECOVERY_OP_MOVE)
            break;
        
        if (FS_FileSeek(recoveryFD, total, FS_SEEK_CUR) < 0)
            break;

        pos += required + total;
        if (pos == length)
        {
            if (op == RECOVERY_OP_DONE)
                break;

            recovery = true;
            FS_FileSeek(recoveryFD, 0, FS_SEEK_SET);
            break;
        }
    }
    
    pos = 0;
    while (recovery)
    {
        required = sizeof(op) + sizeof(total);
        if (FS_FileRead(recoveryFD, (void*) buffer.GetBuffer(), required) != required)
            ASSERT_FAIL();
            
        p = buffer.GetBuffer();
        op = FromLittle32(*((uint32_t*) p));
        p += sizeof(op);
        total = FromLittle64(*((uint64_t*) p));
        pos += required + total;
    
        switch (op)
        {
        case RECOVERY_OP_COPY:
            PerformRecoveryCopy();
            break;
        case RECOVERY_OP_MOVE:
            PerformRecoveryMove();
            break;
        case RECOVERY_OP_CREATE:
            PerformRecoveryCreateShard(oldShardID, newShardID);
            break;
        default:
            ASSERT_FAIL();
        }       

        if (pos == length)
            break;      
    }

    if (recovery)
    {
        DeleteGarbageShard(newShardID);
        RebuildShardTOC(oldShardID);
        RebuildTOC();
    }

    FS_FileSeek(recoveryFD, 0, SEEK_SET);
    FS_FileTruncate(recoveryFD, 0); 
    FS_Sync();
}

void StorageTable::PerformRecoveryCreateShard(uint64_t& oldShardID, uint64_t& newShardID)
{
    int64_t             required;
    Buffer              oldName;
    char*               p;

    required = sizeof(oldShardID) + sizeof(newShardID);
    if (FS_FileRead(recoveryFD, (void*) buffer.GetBuffer(), required) != required)
        ASSERT_FAIL();
    p = buffer.GetBuffer();
    
    oldShardID = FromLittle64(*((uint64_t*) p));
    p += sizeof(oldShardID);

    newShardID = FromLittle64(*((uint64_t*) p));
    p += sizeof(newShardID);
    
    // delete TOC file from old shard
    oldName.Write(path.GetBuffer(), path.GetLength() - 1);
    oldName.Appendf("%U/index", oldShardID);
    oldName.NullTerminate();
    
    FS_Delete(oldName.GetBuffer());
}

void StorageTable::PerformRecoveryCopy()
{
    uint64_t        oldShardID;
    uint32_t        fileIndex;
    uint64_t        length;
    int64_t         required;
    FD              dataFD;
    Buffer          filename;
    Buffer          dirname;
    char*           p;
    const unsigned  bufsize = 64 * 1024;

    required = sizeof(oldShardID) + sizeof(fileIndex) + sizeof(length);
    if (FS_FileRead(recoveryFD, (void*) buffer.GetBuffer(), required) != required)
        ASSERT_FAIL();
    p = buffer.GetBuffer();
    
    oldShardID = FromLittle64(*((uint64_t*) p));
    p += sizeof(oldShardID);
    
    fileIndex = FromLittle32(*((uint32_t*) p));
    p += sizeof(fileIndex);
    
    length = FromLittle64(*((uint64_t*) p));
    p += sizeof(length);
    
    dirname.Write(path.GetBuffer(), path.GetLength() - 1);
    dirname.Appendf("%U", oldShardID);
    dirname.NullTerminate();
    StorageShard::WritePath(filename, dirname, fileIndex);
    
    dataFD = FS_Open(filename.GetBuffer(), FS_READWRITE);
    if (dataFD == INVALID_FD)
        ASSERT_FAIL();
    
    // copy the file back to the original shard
    buffer.Allocate(bufsize);
    required = (int64_t) length;
    while (required > 0)
    {
        unsigned nread = required % bufsize;
        if (FS_FileRead(recoveryFD, (void*) buffer.GetBuffer(), nread) != nread)
            ASSERT_FAIL();
        if (FS_FileWrite(dataFD, (const void *) buffer.GetBuffer(), nread) != nread)
            ASSERT_FAIL();
        required -= nread;
    }
    
    FS_FileClose(dataFD);
}

void StorageTable::PerformRecoveryMove()
{
    uint32_t        oldNameLength;
    uint32_t        newNameLength;
    int64_t         required;
    Buffer          oldName;
    Buffer          newName;
    char*           p;

    required = sizeof(oldNameLength);
    if (FS_FileRead(recoveryFD, (void*) buffer.GetBuffer(), required) != required)
        ASSERT_FAIL();
    p = buffer.GetBuffer();
    
    oldNameLength = FromLittle32(*((uint32_t*) p));
    p += sizeof(oldNameLength);
    
    required = oldNameLength;
    oldName.Allocate(oldNameLength + 1);
    if (FS_FileRead(recoveryFD, (void*) oldName.GetBuffer(), required) != required)
        ASSERT_FAIL();

    oldName.SetLength(oldNameLength);
    oldName.NullTerminate();
    
    required = sizeof(newNameLength);
    if (FS_FileRead(recoveryFD, (void*) buffer.GetBuffer(), required) != required)
        ASSERT_FAIL();
    p = buffer.GetBuffer();
    
    newNameLength = FromLittle32(*((uint32_t*) p));
    p += sizeof(newNameLength);
    required = newNameLength;
    newName.Allocate(newNameLength + 1);
    if (FS_FileRead(recoveryFD, (void*) newName.GetBuffer(), required) != required)
        ASSERT_FAIL();

    newName.SetLength(newNameLength);
    newName.NullTerminate();

    // this might fail if crashed while in recovery, but it is OK
    FS_Rename(newName.GetBuffer(), oldName.GetBuffer());
}

void StorageTable::RebuildTOC()
{
    FS_Dir              dir;
    FS_DirEntry         dirent;
    Buffer              fullname;
    Buffer              buffer;
    unsigned            i;
    StorageShardIndex*  si;
    ReadBuffer          firstKey;
    bool                isShardDir;
    
    dir = FS_OpenDir(path.GetBuffer());
    
    while ((dirent = FS_ReadDir(dir)) != FS_INVALID_DIR_ENTRY)
    {
        fullname.Write(path.GetBuffer(), path.GetLength() - 1);
        fullname.Append(FS_DirEntryName(dirent));
        fullname.NullTerminate();

        if (!FS_IsDirectory(fullname.GetBuffer()))
            continue;

        buffer.Write(FS_DirEntryName(dirent));
        isShardDir = true;
        for (i = 0; i < buffer.GetLength(); i++)
        {
            if (!isdigit(buffer.GetCharAt(i)))
            {
                isShardDir = false;
                break;
            }
        }
        if (!isShardDir)
            continue;
        
        si = new StorageShardIndex;
        si->shard = new StorageShard;
        si->shard->Open(fullname.GetBuffer(), FS_DirEntryName(dirent));
        si->SetStartKey(si->shard->FirstKey(), true);
        si->shard->Close();
        delete si->shard;
        si->shard = NULL;
        shards.Insert(si);
    }
    
    FS_CloseDir(dir);

    WriteTOC(); 
}

void StorageTable::ReadTOC(uint64_t length)
{
    uint32_t            i, numShards;
    unsigned            len;
    char*               p;
    StorageShardIndex*  si;
    int                 ret;
    StorageFileHeader   header;
    Buffer              headerBuf;
    
    headerBuf.Allocate(STORAGEFILE_HEADER_LENGTH);
    if ((ret = FS_FileRead(tocFD, (void*) headerBuf.GetBuffer(), STORAGEFILE_HEADER_LENGTH)) < 0)
        ASSERT_FAIL();
    if (ret != STORAGEFILE_HEADER_LENGTH)
        ASSERT_FAIL();
    headerBuf.SetLength(STORAGEFILE_HEADER_LENGTH);
    if (!header.Read(headerBuf))
        ASSERT_FAIL();
    
    length -= STORAGEFILE_HEADER_LENGTH;
    if ((ret = FS_FileRead(tocFD, (void*) buffer.GetBuffer(), length)) < 0)
        ASSERT_FAIL();
    p = buffer.GetBuffer();
    numShards = FromLittle32(*((uint32_t*) p));
    assert(numShards * 8 + 4 <= length);
    p += 4;
    for (i = 0; i < numShards; i++)
    {
        si = new StorageShardIndex;
        si->shardID = FromLittle32(*((uint32_t*) p));
        p += 4;
        len = FromLittle32(*((uint32_t*) p));
        p += 4;
        si->startKey.SetLength(len);
        si->startKey.SetBuffer(p);
        shards.Insert(si);
    }
}

void StorageTable::WriteTOC()
{
    char*               p;
    uint32_t            size, len;
    uint32_t            tmp;
    StorageShardIndex   *it;
    StorageFileHeader   header;
    Buffer              writeBuf;

    FS_FileSeek(tocFD, 0, FS_SEEK_SET);
    FS_FileTruncate(tocFD, 0);
    
    header.Init(FILE_TYPE, FILE_VERSION_MAJOR, FILE_VERSION_MINOR, 0);
    header.Write(writeBuf);

    if (FS_FileWrite(tocFD, (const void*) writeBuf.GetBuffer(), writeBuf.GetLength()) < 0)
        ASSERT_FAIL();
    
    len = shards.GetCount();
    tmp = ToLittle32(len);

    if (FS_FileWrite(tocFD, (const void *) &tmp, 4) < 0)
        ASSERT_FAIL();

    for (it = shards.First(); it != NULL; it = shards.Next(it))
    {       
        size = 4 + (4+it->startKey.GetLength());
        writeBuf.Allocate(size);
        p = writeBuf.GetBuffer();
        *((uint32_t*) p) = ToLittle32(it->shardID);
        p += 4;
        len = it->startKey.GetLength();
        *((uint32_t*) p) = ToLittle32(len);
        p += 4;
        memcpy(p, it->startKey.GetBuffer(), len);
        p += len;
        if (FS_FileWrite(tocFD, (const void *) writeBuf.GetBuffer(), size) < 0)
            ASSERT_FAIL();
    }
}

void StorageTable::WriteRecoveryDone()
{
    uint32_t        op;
    uint64_t        total;
    char*           p;
    int64_t         required;

    p = buffer.GetBuffer();

    op = RECOVERY_OP_COPY;
    *((uint32_t*) p) = ToLittle32(op);
    p += sizeof(op);
    
    total = 0;
    *((uint64_t*) p) = ToLittle64(total);
    
    required = sizeof(op) + sizeof(total);

    if (FS_FileWrite(recoveryFD, (const void*) p, required) != required)
        ASSERT_FAIL();
    
    FS_FileSeek(recoveryFD, 0, SEEK_SET);
    FS_FileTruncate(recoveryFD, 0); 
    FS_Sync();
}

void StorageTable::WriteRecoveryCreateShard(uint64_t oldShardID, uint64_t newShardID)
{
    uint32_t        op;
    uint64_t        total;
    char*           p;
    int64_t         required;

    p = buffer.GetBuffer();

    op = RECOVERY_OP_CREATE;
    *((uint32_t*) p) = ToLittle32(op);
    p += sizeof(op);
    
    total = sizeof(oldShardID) + sizeof(newShardID);
    *((uint64_t*) p) = ToLittle64(total);
    p += sizeof(total);

    *((uint64_t*) p) = ToLittle64(oldShardID);
    p += sizeof(oldShardID);
    
    *((uint64_t*) p) = ToLittle64(newShardID);
    p += sizeof(newShardID);
    
    required = sizeof(op) + sizeof(total) + total;

    if (FS_FileWrite(recoveryFD, (const void*) buffer.GetBuffer(), required) != required)
        ASSERT_FAIL();
}

void StorageTable::WriteRecoveryCopy(uint64_t oldShardID, uint32_t fileIndex)
{
    uint32_t        op;
    uint64_t        length;
    uint64_t        total;
    int64_t         required;
    FD              dataFD;
    Buffer          filename;
    Buffer          dirname;
    int64_t         ret;
    char*           p;
    const unsigned  bufsize = 64 * 1024;

    dirname.Write(path.GetBuffer(), path.GetLength() - 1);
    dirname.Appendf("%U", oldShardID);
    dirname.NullTerminate();
    StorageShard::WritePath(filename, dirname, fileIndex);

    dataFD = FS_Open(filename.GetBuffer(), FS_READONLY);
    if (dataFD == INVALID_FD)
        ASSERT_FAIL();
    
    ret = FS_FileSize(tocFD);
    if (ret < 0)
        ASSERT_FAIL();
    
    length = (uint64_t) ret;
    
    required = sizeof(op) + sizeof(total) + 
                sizeof(oldShardID) + sizeof(fileIndex) + sizeof(length);
    total = sizeof(oldShardID) + sizeof(fileIndex) + sizeof(length) + length;
    buffer.Allocate(required);
    
    p = buffer.GetBuffer();

    op = RECOVERY_OP_COPY;
    *((uint32_t*) p) = ToLittle32(op);
    p += sizeof(op);

    *((uint64_t*) p) = ToLittle64(total);
    p += sizeof(total);
    
    *((uint64_t*) p) = ToLittle64(oldShardID);
    p += sizeof(oldShardID);
    
    *((uint32_t*) p) = ToLittle32(fileIndex);
    p += sizeof(fileIndex);
    
    *((uint64_t*) p) = ToLittle64(length);
    p += sizeof(length);
    
    if (FS_FileWrite(recoveryFD, (const void*) p, required) != required)
        ASSERT_FAIL();
    
    buffer.Allocate(bufsize);
    required = length;
    while (required > 0)
    {
        unsigned nread = required % bufsize;
        if (FS_FileRead(dataFD, (void*) buffer.GetBuffer(), nread) != nread)
            ASSERT_FAIL();
        if (FS_FileWrite(recoveryFD, (const void *) buffer.GetBuffer(), nread) != nread)
            ASSERT_FAIL();
        required -= nread;
    }

    FS_FileClose(dataFD);
}

void StorageTable::WriteRecoveryMove(Buffer& src, Buffer& dst)
{
    uint32_t        op;
    uint64_t        total;
    uint32_t        oldNameLength;
    uint32_t        newNameLength;
    int64_t         required;
    char*           p;
    
    op = RECOVERY_OP_MOVE;

    p = buffer.GetBuffer();
    *((uint32_t*) p) = ToLittle32(op);
    required = sizeof(op);  
    if (FS_FileWrite(recoveryFD, (const void*) p, required) != required)
        ASSERT_FAIL();

    total = sizeof(oldNameLength) + src.GetLength() - 1 +
            sizeof(newNameLength) + dst.GetLength() - 1;
    
    p = buffer.GetBuffer();
    *((uint64_t*) p) = ToLittle64(total);
    required = sizeof(total);
    if (FS_FileWrite(recoveryFD, (const void*) p, required) != required)
        ASSERT_FAIL();
    
    oldNameLength = src.GetLength() - 1;
    p = buffer.GetBuffer();
    *((uint32_t*) p) = ToLittle32(oldNameLength);
    required = sizeof(oldNameLength);
    if (FS_FileWrite(recoveryFD, (const void*) p, required) != required)
        ASSERT_FAIL();
    
    p = src.GetBuffer();
    required = oldNameLength;
    if (FS_FileWrite(recoveryFD, (const void*) p, required) != required)
        ASSERT_FAIL();
        
    newNameLength = dst.GetLength() - 1;
    p = buffer.GetBuffer();
    *((uint32_t*) p) = ToLittle32(newNameLength);
    required = sizeof(newNameLength);
    if (FS_FileWrite(recoveryFD, (const void*) p, required) != required)
        ASSERT_FAIL();

    p = dst.GetBuffer();
    required = newNameLength;
    if (FS_FileWrite(recoveryFD, (const void*) p, required) != required)
        ASSERT_FAIL();
}

void StorageTable::DeleteGarbageShard(uint64_t shardID)
{
    const char*     p;
    FS_Dir          dir;
    FS_DirEntry     dirent;
    Buffer          tmp;
    Buffer          dirname;
    char            sep;
    
    sep = FS_Separator();
        
    dirname.Write(path.GetBuffer(), path.GetLength() - 1);
    dirname.Appendf("%U", shardID);
    dirname.NullTerminate();

    dir = FS_OpenDir(dirname.GetBuffer());
    
    while ((dirent = FS_ReadDir(dir)) != FS_INVALID_DIR_ENTRY)
    {
        p = FS_DirEntryName(dirent);
        if (p[0] == '.')
            continue;

        tmp.Write(dirname.GetBuffer(), dirname.GetLength() - 1);
        tmp.Append(&sep, 1);
        tmp.Append(p);
        tmp.NullTerminate();

        FS_Delete(tmp.GetBuffer());
    }

    FS_CloseDir(dir);
    FS_DeleteDir(dirname.GetBuffer());
}

void StorageTable::RebuildShardTOC(uint64_t shardID)
{
    StorageShardIndex*  si;
    Buffer              fullname;
    Buffer              shardName;
    
    fullname.Write(path.GetBuffer(), path.GetLength() - 1);
    fullname.Appendf("%U", shardID);
    fullname.NullTerminate();
    
    shardName.Writef("%U", shardID);
    shardName.NullTerminate();
    
    si = new StorageShardIndex;
    si->shard = new StorageShard;
    si->shard->Open(fullname.GetBuffer(), shardName.GetBuffer());
    si->shard->RebuildTOC();
    si->shard->Close();
    delete si;
}

void StorageTable::CommitPhase1()
{
    StorageShardIndex*  si;
    
    for (si = shards.First(); si != NULL; si = shards.Next(si))
    {
        if (si->shard != NULL)
            si->shard->CommitPhase1();
    }
}

void StorageTable::CommitPhase2()
{
    StorageShardIndex*  si;
    
    for (si = shards.First(); si != NULL; si = shards.Next(si))
    {
        if (si->shard != NULL)
            si->shard->CommitPhase2();
    }
}

void StorageTable::CommitPhase3()
{
    StorageShardIndex*  si;
    
    for (si = shards.First(); si != NULL; si = shards.Next(si))
    {
        if (si->shard != NULL)
            si->shard->CommitPhase3();
    }
}

void StorageTable::CommitPhase4()
{
    StorageShardIndex*  si;
    
    for (si = shards.First(); si != NULL; si = shards.Next(si))
    {
        if (si->shard != NULL)
            si->shard->CommitPhase4();
    }
}


