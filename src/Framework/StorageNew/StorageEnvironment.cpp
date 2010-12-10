#include "StorageEnvironment.h"

StorageEnvironment::StorageEnvironment()
{
    commitThread = NULL;
    backgroundThread = NULL;
    backgroundTimer.SetDelay(1000);
    backgroundTimer.SetCallable(MFUNC(StorageEnvironment, OnBackgroundTimeout));    
}

bool StorageEnvironment::Open(const char* filepath)
{
    // TODO
    
    commitThread = ThreadPool::Create(1);
    commitThread->Create();
    
    backgroundThread = ThreadPool::Create(1);
    backgroundThread->Create();
    EventLoop::Add(&backgroundTimer);
}

void StorageEnvironment::Close()
{
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
    StorageChunk*       itChunk;
    StorageKeyValue*    kv;
    ReadBuffer          firstKey;
    ReadBuffer          lastKey;

    shard = GetShard(shardID);
    if (shard == NULL)
        return false;

    firstKey = shard->GetFirstKey();
    lastKey = shard->GetLastKey();

    FOREACH(itChunk, shard->chunks)
    {
        kv = itChunk->Get(firstKey, lastKey, key);
        if (kv != NULL)
        {
            if (kv.IsDelete())
            {
                return false;
            }
            if (kv.IsSet())
            {
                value.Wrap(kv->GetValue());
                return true;
            }
        }
    }

    return false;
}

bool StorageEnvironment::Set(uint64_t shardID, ReadBuffer& key, ReadBuffer& value)
{
    uint64_t        commandID;
    StorageShard*   shard;
    StorageChunk*   chunk;
    
    if (commitThread->NumActive() > 0)
        return false;

    shard = GetShard(shardID);
    if (shard == NULL)
        return false;

    commandID = activeLogSegment->AppendSet(shardID, key, value);
    if (commandID < 0)
        return false;

    chunk = shard->GetWriteChunk();
    assert(chunk != NULL);
    if (!chunk->Set(key, value))
    {
        activeLogSegment->Undo();
        return false;
    }
    chunk->RegisterLogCommand(activeLogSegment->GetLogSegmentID(), commandID);

    return true;
}

bool StorageEnvironment::Delete(uint64_t shardID, ReadBuffer& key)
{
    StorageShard* shard;

    if (commitThread->NumActive() > 0)
        return false;

    shard = GetShard(shardID);
    if (shard == NULL)
        return false;

    commandID = activeLogSegment->AppendDelete(shardID, key, value);
    if (commandID < 0)
        return false;

    chunk = shard->GetWriteChunk();
    assert(chunk != NULL);
    if (!chunk->Delete(key))
    {
        activeLogSegment->Undo();
        return false;
    }
    chunk->RegisterLogCommand(activeLogSegment->GetLogSegmentID(), commandID);

    return true;
}

void StorageEnvironment::SetOnCommit(Callable& onCommit_)
{
    onCommit = onCommit_;
}

void StorageEnvironment::Commit()
{
    if (commitThread->NumActive() > 0)
        return false;

    MFunc<StorageLogSegmentWriter, &StorageLogSegmentWriter::Commit> callable(activeLogSegment);        
    commitThread.Execute(callable);
}

bool StorageEnvironment::GetCommitStatus()
{
    return activeLogSegment->GetCommitStatus();
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

StorageChunk* StorageEnvironment::GetChunk(uint64_t chunkID)
{
    StorageChunk* it;

    FOREACH(it, chunks)
    {
        if (it->GetChunkID() == chunkID)
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
    StorageChunk*   it;
    StorageJob*     job;
    
    if (backgroundThread.GetNumActive() > 0)
        return;
    
    FOREACH(it, chunks)
    {
        if (it->GetState() == ChunkState::Serialized)
        {
            job = new StorageWriteChunkJob(it);
            StartJob(job);
        }
    }
}

void StorageEnvironment::TryFinalizeLogSegment()
{
    uint64_t    logSegmentID;
    Buffer      filename;

    if (activeLogSegment->GetOffset() < config.logSegmentSize)
        return;

    activeLogSegment->Finalize();
    activeLogSegment->Close();

    logSegmentID = activeLogSegment->GetLogSegmentID();
    logSegmentID++;

    logSegments.Append(activeLogSegment);

    headLogSegment = new StorageLogSegment();
    headLogSegment->SetLogSegmentID(logSegmentID);
    buffer.Writef("log.%020U", logSegmentID);
    buffer.NullTerminate();
    headLogSegment->Open(buffer);

    logSegmentID++;
}

void StorageEnvironment::TryFinalizeChunks()
{
    StorageChunk*   itChunk;
    StorageShard*   itShard;
    StorageChunk*   newChunk;

    FOREACH(itChunk, chunks)
    {
        if (itChunk->GetSize() > config.chunkSize)
            itChunk->Finalize();
    }

    FOREACH(itShard, shards)
    {
        if (!itShard->GetWriteChunk()->IsFinalized())
            continue;

        newChunk = new StorageChunk;
        newChunk->SetChunkID(NextChunkID(activeChunk->GetChunkID()));
        newChunk->SetBloomFilter(itShard->UseBloomFilter());
        itShard->SetNewWriteChunk(newChunk);
        chunks.Append(newChunk);
    }
}

bool StorageEnvironment::IsWriteActive()
{

}

void StorageEnvironment::StartJob(StorageJob* job)
{
    MFunc<StorageJob, &StorageJob::Execute> callable(job);        
    backgroundThread.Execute(callable);
}