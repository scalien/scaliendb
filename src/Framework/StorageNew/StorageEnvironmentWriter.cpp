#include "StorageEnvironmentWriter.h"
#include "System/FileSystem.h"
#include "StorageEnvironment.h"

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
    
    FS_FileTruncate(fd.GetFD(), 0);
    FS_Sync(fd.GetFD());
    
    writeSize = writeBuffer.GetLength();
    if (FS_FileWrite(fd.GetFD(), writeBuffer.GetBuffer(), writeSize) != writeSize)
        return false;

    FS_Sync(fd.GetFD());
    fd.Close();

//    if (!FS_Delete(TOC.GetBuffer()))
//        return false;
    
    FS_Rename(newTOC.GetBuffer(), TOC.GetBuffer());
    
    return true;
}

void StorageEnvironmentWriter::WriteBuffer()
{
    uint32_t        numShards, checksum, length;
    StorageShard*   itShard;
    ReadBuffer      dataPart;
    
    writeBuffer.Allocate(4096);
    writeBuffer.AppendLittle32(0);  // dummy for size
    writeBuffer.AppendLittle32(0);  // dummy for CRC
    
    numShards = env->shards.GetLength();
    writeBuffer.AppendLittle32(numShards);
    FOREACH (itShard, env->shards)
    {
        WriteShard(itShard);
    }
    
    length = writeBuffer.GetLength();
    dataPart.SetBuffer(writeBuffer.GetBuffer() + 8);
    dataPart.SetLength(length - 8);
    checksum = dataPart.GetChecksum();
    
    writeBuffer.SetLength(0);
    writeBuffer.AppendLittle32(length);
    writeBuffer.AppendLittle32(checksum);
    writeBuffer.SetLength(length);
    
}

void StorageEnvironmentWriter::WriteShard(StorageShard* shard)
{
    Buffer          tmp;
    uint32_t        numChunks;
    ReadBuffer      firstKey, lastKey;
    StorageChunk**  itChunk;
    
    firstKey = shard->GetFirstKey();
    lastKey = shard->GetLastKey();
    
    writeBuffer.AppendLittle16(shard->GetContextID());
    writeBuffer.AppendLittle64(shard->GetTableID());
    writeBuffer.AppendLittle64(shard->GetShardID());
    writeBuffer.Appendf("%#R", &firstKey);
    writeBuffer.Appendf("%#R", &lastKey);
    writeBuffer.Appendf("%b", shard->UseBloomFilter());
    
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
