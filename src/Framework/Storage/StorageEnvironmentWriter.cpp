#include "StorageEnvironmentWriter.h"
#include "System/FileSystem.h"
#include "StorageEnvironment.h"

#define STORAGE_TOC_VERSION         1
#define STORAGE_TOC_HEADER_SIZE     12

bool StorageEnvironmentWriter::Write(StorageEnvironment* env_)
{
    Buffer      newTOC;
    Buffer      TOC;
    uint32_t    writeSize;
 
    env = env_;
    
    WriteBuffer();
    
    newTOC.Write(env->envPath);
    newTOC.Append("toc.new");
    newTOC.NullTerminate();
    
    TOC.Write(env->envPath);
    TOC.Append("toc");
    TOC.NullTerminate();
    
    if (fd.Open(newTOC.GetBuffer(), FS_CREATE | FS_READWRITE) == INVALID_FD)
        return false;
    
    // TODO: open with FS_TRUNCATE flag
    FS_FileTruncate(fd.GetFD(), 0);
    StorageEnvironment::Sync(fd.GetFD());
    
    writeSize = writeBuffer.GetLength();
    if (FS_FileWrite(fd.GetFD(), writeBuffer.GetBuffer(), writeSize) != (ssize_t) writeSize)
        return false;

    StorageEnvironment::Sync(fd.GetFD());
    fd.Close();

    FS_Delete(TOC.GetBuffer());
    
    FS_Rename(newTOC.GetBuffer(), TOC.GetBuffer());
    
    return true;
}

uint64_t StorageEnvironmentWriter::WriteSnapshot(StorageEnvironment* env_)
{
    Buffer      TOC;
    uint32_t    writeSize;
    uint64_t    tocID;
 
    env = env_;
    
    WriteBuffer();
    
    tocID = Now();
    TOC.Write(env->envPath);
    TOC.Appendf("toc.%U", tocID);
    TOC.NullTerminate();
    
    if (fd.Open(TOC.GetBuffer(), FS_CREATE | FS_READWRITE) == INVALID_FD)
        return false;
    
    // TODO: open with FS_TRUNCATE flag
    FS_FileTruncate(fd.GetFD(), 0);
    StorageEnvironment::Sync(fd.GetFD());
    
    writeSize = writeBuffer.GetLength();
    if (FS_FileWrite(fd.GetFD(), writeBuffer.GetBuffer(), writeSize) != (ssize_t) writeSize)
        return false;

    StorageEnvironment::Sync(fd.GetFD());
    fd.Close();

    return tocID;
}

bool StorageEnvironmentWriter::DeleteSnapshot(StorageEnvironment* env, uint64_t tocID)
{
    Buffer      TOC;

    TOC.Write(env->envPath);
    TOC.Appendf("toc.%U", tocID);
    TOC.NullTerminate();

    return FS_Delete(TOC.GetBuffer());
}

void StorageEnvironmentWriter::WriteBuffer()
{
    uint32_t        numShards, checksum, length;
    StorageShard*   itShard;
    ReadBuffer      dataPart;
    
    writeBuffer.Allocate(4096);
    writeBuffer.AppendLittle32(0);  // dummy for size
    writeBuffer.AppendLittle32(0);  // dummy for CRC
    writeBuffer.AppendLittle32(STORAGE_TOC_VERSION);
    
    numShards = env->shards.GetLength();
    writeBuffer.AppendLittle32(numShards);
    FOREACH (itShard, env->shards)
        WriteShard(itShard);
    
    length = writeBuffer.GetLength();
    dataPart.SetBuffer(writeBuffer.GetBuffer() + 8);
    dataPart.SetLength(length - 8);
    checksum = dataPart.GetChecksum();
    
    writeBuffer.SetLength(0);
    writeBuffer.AppendLittle32(length);
    writeBuffer.AppendLittle32(checksum);
    writeBuffer.SetLength(length);
    // VERSION==0 ends here
}

void StorageEnvironmentWriter::WriteShard(StorageShard* shard)
{
    Buffer          tmp;
    uint32_t        numChunks;
    ReadBuffer      firstKey, lastKey;
    StorageChunk**  itChunk;
    
    firstKey = shard->GetFirstKey();
    lastKey = shard->GetLastKey();
    
    writeBuffer.AppendLittle64(shard->GetTrackID());
    writeBuffer.AppendLittle16(shard->GetContextID());
    writeBuffer.AppendLittle64(shard->GetTableID());
    writeBuffer.AppendLittle64(shard->GetShardID());
    writeBuffer.AppendLittle64(shard->GetLogSegmentID());
    writeBuffer.AppendLittle32(shard->GetLogCommandID());
    writeBuffer.Appendf("%#R", &firstKey);
    writeBuffer.Appendf("%#R", &lastKey);
    writeBuffer.Appendf("%b", shard->UseBloomFilter());
    writeBuffer.Appendf("%c", shard->GetStorageType());
    
    numChunks = 0;
    FOREACH (itChunk, shard->GetChunks())
    {
        if ((*itChunk)->GetChunkState() == StorageChunk::Written)
        {
            numChunks++;
            tmp.AppendLittle64((*itChunk)->GetChunkID());
        }
    }
    
    writeBuffer.AppendLittle32(numChunks);
    writeBuffer.Append(tmp);
}
