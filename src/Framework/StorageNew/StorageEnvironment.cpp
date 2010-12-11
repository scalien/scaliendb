#include "StorageEnvironment.h"
#include "System/Events/EventLoop.h"
#include "System/FileSystem.h"
#include "StorageChunkSerializer.h"

StorageEnvironment::StorageEnvironment()
{
    commitThread = NULL;
    backgroundThread = NULL;
    backgroundTimer.SetDelay(1000);
    backgroundTimer.SetCallable(MFUNC(StorageEnvironment, OnBackgroundTimeout));    
}

bool StorageEnvironment::Open(Buffer& envPath_)
{
    char    lastChar;
    Buffer  tmp;

    commitThread = ThreadPool::Create(1);
    commitThread->Start();
    
    backgroundThread = ThreadPool::Create(1);
    backgroundThread->Start();
    EventLoop::Add(&backgroundTimer);

    envPath.Write(envPath_);
    lastChar = envPath.GetCharAt(envPath.GetLength() - 1);
    if (lastChar != '/' && lastChar != '\\')
        envPath.Append('/');

    chunkPath.Write(envPath);
    chunkPath.Append("chunks/");

    logPath.Write(envPath);
    logPath.Append("log/");

    tmp.Write(envPath);
    tmp.NullTerminate();
    FS_CreateDir(tmp.GetBuffer());

    tmp.Write(chunkPath);
    tmp.NullTerminate();
    FS_CreateDir(tmp.GetBuffer());

    tmp.Write(logPath);
    tmp.NullTerminate();
    FS_CreateDir(tmp.GetBuffer());

    return true;
}

void StorageEnvironment::Close()
{
    commitThread->Stop();
    backgroundThread->Stop();
}

void StorageEnvironment::SetStorageConfig(StorageConfig& config_)
{
    config = config_;
}

uint64_t StorageEnvironment::GetShardID(uint64_t tableID, ReadBuffer& key)
{
    StorageShard* it;

    FOREACH(it, shards)
    {
        if (it->GetTableID() == tableID)
        {
            if (it->RangeContains(key))
                return it->GetShardID();
        }
    }

    return 0;
}

bool StorageEnvironment::Get(uint64_t shardID, ReadBuffer& key, ReadBuffer& value)
{
    StorageShard*       shard;
    StorageChunk**      itChunk;
    StorageKeyValue*    kv;

    shard = GetShard(shardID);
    if (shard == NULL)
        return false;

    BFOREACH(itChunk, (*shard))
    {
        kv = (*itChunk)->Get(key);
        if (kv != NULL)
        {
            if (kv->GetType() == STORAGE_KEYVALUE_TYPE_DELETE)
            {
                return false;
            }
            else if (kv->GetType() == STORAGE_KEYVALUE_TYPE_SET)
            {
                value = kv->GetValue();
                return true;
            }
            else
                ASSERT_FAIL();
        }
    }

    return false;
}

bool StorageEnvironment::Set(uint64_t shardID, ReadBuffer& key, ReadBuffer& value)
{
    uint64_t            commandID;
    StorageShard*       shard;
    StorageMemoChunk*   chunk;
    
    if (commitThread->GetNumActive() > 0)
        return false;

    shard = GetShard(shardID);
    if (shard == NULL)
        return false;

    commandID = logSegmentWriter->AppendSet(shardID, key, value);
    if (commandID < 0)
        return false;

    chunk = shard->GetMemoChunk();
    assert(chunk != NULL);
    if (!chunk->Set(key, value))
    {
        logSegmentWriter->Undo();
        return false;
    }
    chunk->RegisterLogCommand(logSegmentWriter->GetLogSegmentID(), commandID);

    return true;
}

bool StorageEnvironment::Delete(uint64_t shardID, ReadBuffer& key)
{
    uint64_t            commandID;
    StorageShard*       shard;
    StorageMemoChunk*   chunk;

    if (commitThread->GetNumActive() > 0)
        return false;

    shard = GetShard(shardID);
    if (shard == NULL)
        return false;

    commandID = logSegmentWriter->AppendDelete(shardID, key);
    if (commandID < 0)
        return false;

    chunk = shard->GetMemoChunk();
    assert(chunk != NULL);
    if (!chunk->Delete(key))
    {
        logSegmentWriter->Undo();
        return false;
    }
    chunk->RegisterLogCommand(logSegmentWriter->GetLogSegmentID(), commandID);

    return true;
}

void StorageEnvironment::SetOnCommit(Callable& onCommit_)
{
    onCommit = onCommit_;
}

bool StorageEnvironment::Commit()
{
    if (commitThread->GetNumActive() > 0)
        return false;

    MFunc<StorageLogSegmentWriter, &StorageLogSegmentWriter::Commit> callable(logSegmentWriter);        
    commitThread->Execute(callable);

    return true;
}

bool StorageEnvironment::GetCommitStatus()
{
    return logSegmentWriter->GetCommitStatus();
}

StorageShard* StorageEnvironment::GetShard(uint64_t shardID)
{
    StorageShard* it;

    FOREACH(it, shards)
    {
        if (it->GetShardID() == shardID)
            return it;
    }

    return NULL;
}

void StorageEnvironment::OnCommit()
{
    TryFinalizeLogSegment();
    TryFinalizeChunks();
}

void StorageEnvironment::OnBackgroundTimeout()
{
    StorageFileChunk*   it;
    StorageJob*         job;
    
    if (backgroundThread->GetNumActive() > 0)
        return;
    
    FOREACH(it, fileChunks)
    {
        if (!it->IsWritten())
        {
            job = new StorageWriteChunkJob(it);
            StartJob(job);
            return;
        }
    }
}

void StorageEnvironment::TryFinalizeLogSegment()
{
    uint64_t    logSegmentID;
    Buffer      filename;

    if (logSegmentWriter->GetOffset() < config.logSegmentSize)
        return;

    logSegmentWriter->Close();

    logSegmentID = logSegmentWriter->GetLogSegmentID();
    logSegmentID++;

    delete logSegmentWriter; // this is wrong
// TODO:    logSegments.Append(logSegmentWriter);

    logSegmentWriter = new StorageLogSegmentWriter();
    filename.Writef("log.%020U", logSegmentID);
    logSegmentWriter->Open(filename, logSegmentID);

    logSegmentID++;
}

void StorageEnvironment::TryFinalizeChunks()
{
    StorageShard*               itShard;
    StorageMemoChunk*           memoChunk;
    StorageFileChunk*           fileChunk;
    StorageChunkSerializer      serializer;

    FOREACH(itShard, shards)
    {
        memoChunk = itShard->GetMemoChunk();
        if (memoChunk->GetSize() > config.chunkSize)
        {
            fileChunk = serializer.Serialize(memoChunk);
            if (fileChunk == NULL)
            {
                // TODO
            }

            delete memoChunk;
        }
    }

    //FOREACH(itShard, shards)
    //{
    //    if (!itShard->GetMemoChunk()->IsFinalized())
    //        continue;

    //    newChunk = new StorageChunk;
    //    newChunk->SetChunkID(NextChunkID(activeChunk->GetChunkID()));
    //    newChunk->SetBloomFilter(itShard->UseBloomFilter());
    //    itShard->SetNewWriteChunk(newChunk);
    //    chunks.Append(newChunk);
    //}
}

bool StorageEnvironment::IsWriteActive()
{
    return (commitThread->GetNumActive() > 0);
}

void StorageEnvironment::StartJob(StorageJob* job)
{
    MFunc<StorageJob, &StorageJob::Execute> callable(job);        
    backgroundThread->Execute(callable);
}