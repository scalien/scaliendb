#include "StorageEnvironment.h"
#include "System/Events/EventLoop.h"
#include "System/FileSystem.h"
#include "System/Config.h"
#include "StorageChunkSerializer.h"
#include "StorageEnvironmentWriter.h"
#include "StorageRecovery.h"
#include "StoragePageCache.h"

static inline int KeyCmp(const ReadBuffer& a, const ReadBuffer& b)
{
    return ReadBuffer::Cmp(a, b);
}

static inline const ReadBuffer Key(StorageMemoKeyValue* kv)
{
    return kv->GetKey();
}

StorageEnvironment::StorageEnvironment()
{
    commitThread = NULL;
    serializerThread = NULL;
    writerThread = NULL;
    asyncThread = NULL;

    onChunkSerialize = MFUNC(StorageEnvironment, OnChunkSerialize);
    onChunkWrite = MFUNC(StorageEnvironment, OnChunkWrite);
    onLogArchive = MFUNC(StorageEnvironment, OnLogArchive);
    
    nextChunkID = 1;
    nextLogSegmentID = 1;
    asyncLogSegmentID = 0;
    asyncWriteChunkID = 0;
    
    asyncCommit = false;
}

bool StorageEnvironment::Open(Buffer& envPath_)
{
    char            lastChar;
    Buffer          tmp;
    StorageRecovery recovery;

    commitThread = ThreadPool::Create(1);
    commitThread->Start();
    commitThreadActive = false;
    
    serializerThread = ThreadPool::Create(1);
    serializerThread->Start();
    serializerThreadActive = false;

    writerThread = ThreadPool::Create(1);
    writerThread->Start();
    writerThreadActive = false;

    archiverThread = ThreadPool::Create(1);
    archiverThread->Start();
    archiverThreadActive = false;

    asyncThread = ThreadPool::Create(1);
    asyncThread->Start();

    envPath.Write(envPath_);
    lastChar = envPath.GetCharAt(envPath.GetLength() - 1);
    if (lastChar != '/' && lastChar != '\\')
        envPath.Append('/');

    chunkPath.Write(envPath);
    chunkPath.Append("chunks/");

    logPath.Write(envPath);
    logPath.Append("logs/");

    archivePath.Write(envPath);
    archivePath.Append("archives/");

    archiveScript = configFile.GetValue("database.archiveScript", "$delete");

    tmp.Write(envPath);
    tmp.NullTerminate();
    if (!FS_IsDirectory(tmp.GetBuffer()))
    {
        if (!FS_CreateDir(tmp.GetBuffer()))
        {
            Log_Message("Unable to create environment directory: %s", tmp.GetBuffer());
            Log_Message("Exiting...");
            ASSERT_FAIL();
        }
    }
    
    tmp.Write(chunkPath);
    tmp.NullTerminate();
    if (!FS_IsDirectory(tmp.GetBuffer()))
    {
        if (!FS_CreateDir(tmp.GetBuffer()))
        {
            Log_Message("Unable to create chunk directory: %s", tmp.GetBuffer());
            Log_Message("Exiting...");
            ASSERT_FAIL();
        }
    }

    tmp.Write(logPath);
    tmp.NullTerminate();
    if (!FS_IsDirectory(tmp.GetBuffer()))
    {
        if (!FS_CreateDir(tmp.GetBuffer()))
        {
            Log_Message("Unable to create log directory: %s", tmp.GetBuffer());
            Log_Message("Exiting...");
            ASSERT_FAIL();
        }
    }

    tmp.Write(archivePath);
    tmp.NullTerminate();
    if (!FS_IsDirectory(tmp.GetBuffer()))
    {
        if (!FS_CreateDir(tmp.GetBuffer()))
        {
            Log_Message("Unable to create archive directory: %s", tmp.GetBuffer());
            Log_Message("Exiting...");
            ASSERT_FAIL();
        }
    }
    
    if (!recovery.TryRecovery(this))
    {
        Log_Message("New environment opened.");
    }
    else
    {
        Log_Message("Existing environment opened.");
    }

    headLogSegment = new StorageLogSegment;
    tmp.Write(logPath);
    tmp.Appendf("log.%020U", nextLogSegmentID);
    headLogSegment->Open(tmp, nextLogSegmentID);
    nextLogSegmentID++;

    return true;
}

void StorageEnvironment::Close()
{
    commitThread->Stop();
    delete commitThread;
    commitThreadActive = false;
    serializerThread->Stop();
    delete serializerThread;
    serializerThreadActive = false;
    writerThread->Stop();
    delete writerThread;
    writerThreadActive = false;
    archiverThread->Stop();
    delete archiverThread;
    archiverThreadActive = false;
    asyncThread->Stop();
    delete asyncThread;
    
    shards.DeleteList();
    delete headLogSegment;
    logSegments.DeleteList();

    // TODO: clean up fileChunks properly, see the issue with StorageShard::chunkList
    //fileChunks.DeleteList();
    fileChunks.ClearMembers();
}

void StorageEnvironment::SetStorageConfig(StorageConfig& config_)
{
    config = config_;
}

StorageConfig& StorageEnvironment::GetStorageConfig()
{
    return config;
}

uint64_t StorageEnvironment::GetShardID(uint16_t contextID, uint64_t tableID, ReadBuffer& key)
{
    StorageShard* it;

    FOREACH (it, shards)
    {
        if (it->GetContextID() == contextID && it->GetTableID() == tableID)
        {
            if (it->RangeContains(key))
                return it->GetShardID();
        }
    }

    return 0;
}

bool StorageEnvironment::Get(uint16_t contextID, uint64_t shardID, ReadBuffer key, ReadBuffer& value)
{
    StorageShard*       shard;
    StorageChunk*       chunk;
    StorageChunk**      itChunk;
    StorageKeyValue*    kv;

    StoragePageCache::TryUnloadPages(config);

    shard = GetShard(contextID, shardID);
    if (shard == NULL)
        return false;
        
    if (!shard->RangeContains(key))
        return false;

    chunk = shard->GetMemoChunk();
    kv = chunk->Get(key);
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

    FOREACH_BACK (itChunk, shard->GetChunks())
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

bool StorageEnvironment::Set(uint16_t contextID, uint64_t shardID, ReadBuffer key, ReadBuffer value)
{
    int32_t             logCommandID;
    StorageShard*       shard;
    StorageMemoChunk*   memoChunk;
    
    if (commitThreadActive)
        return false;

    StoragePageCache::TryUnloadPages(config);

    shard = GetShard(contextID, shardID);
    if (shard == NULL)
        return false;

    logCommandID = headLogSegment->AppendSet(contextID, shardID, key, value);
    if (logCommandID < 0)
        return false;

    memoChunk = shard->GetMemoChunk();
    assert(memoChunk != NULL);
    if (!memoChunk->Set(key, value))
    {
        headLogSegment->Undo();
        return false;
    }
    memoChunk->RegisterLogCommand(headLogSegment->GetLogSegmentID(), logCommandID);

    return true;
}

bool StorageEnvironment::Delete(uint16_t contextID, uint64_t shardID, ReadBuffer key)
{
    int32_t             logCommandID;
    StorageShard*       shard;
    StorageMemoChunk*   memoChunk;

    if (commitThreadActive)
        return false;

    StoragePageCache::TryUnloadPages(config);

    shard = GetShard(contextID, shardID);
    if (shard == NULL)
        return false;

    logCommandID = headLogSegment->AppendDelete(contextID, shardID, key);
    if (logCommandID < 0)
        return false;

    memoChunk = shard->GetMemoChunk();
    assert(memoChunk != NULL);
    if (!memoChunk->Delete(key))
    {
        headLogSegment->Undo();
        return false;
    }
    memoChunk->RegisterLogCommand(headLogSegment->GetLogSegmentID(), logCommandID);

    return true;
}

StorageBulkCursor* StorageEnvironment::GetBulkCursor(uint16_t contextID, uint64_t shardID)
{
    StorageShard*       shard;
    StorageBulkCursor*  bc;

    shard = GetShard(contextID, shardID);
    if (!shard)
        return NULL;
    
    bc = new StorageBulkCursor();
    
    bc->SetEnvironment(this);
    bc->SetShard(shard);
    
    return bc;
}

uint64_t StorageEnvironment::GetShardSize(uint16_t contextID, uint64_t shardID)
{
    StorageShard*   shard;
    StorageChunk*   chunk;
    StorageChunk**  itChunk;
    uint64_t        size;

    shard = GetShard(contextID, shardID);
    if (!shard)
        return 0;

    size = 0;
    FOREACH(itChunk, shard->GetChunks())
    {
        chunk = *itChunk;
        size += chunk->GetSize();
    }
    
    return size;
}

void StorageEnvironment::SetOnCommit(Callable& onCommit_)
{
    onCommit = onCommit_;
    asyncCommit = true;
}

bool StorageEnvironment::Commit()
{
    StoragePageCache::TryUnloadPages(config);

    Log_Message("DB Cache size: %s", HUMAN_BYTES(StoragePageCache::GetSize()));

    if (asyncCommit)
    {
        if (commitThreadActive > 0)
            return false;

        MFunc<StorageLogSegment, &StorageLogSegment::Commit> callable(headLogSegment);        
        commitThread->Execute(callable);
        commitThreadActive = true;
    }
    else
    {
        headLogSegment->Commit();
        OnCommit();
    }

    return true;
}

bool StorageEnvironment::GetCommitStatus()
{
    return headLogSegment->GetCommitStatus();
}

StorageShard* StorageEnvironment::GetShard(uint16_t contextID, uint64_t shardID)
{
    StorageShard* it;

    FOREACH (it, shards)
    {
        if (it->GetContextID() == contextID && it->GetShardID() == shardID)
            return it;
    }

    return NULL;
}

bool StorageEnvironment::CreateShard(uint16_t contextID, uint64_t shardID, uint64_t tableID,
 ReadBuffer firstKey, ReadBuffer lastKey, bool useBloomFilter)
{
    StorageShard*       shard;
    StorageMemoChunk*   memoChunk;

    if (headLogSegment->HasUncommitted())
        return false;       // meta writes must occur in-between data writes (commits)
    
    shard = GetShard(contextID, shardID);
    if (shard != NULL)
        return false;       // already exists

    shard = new StorageShard;
    shard->SetContextID(contextID);
    shard->SetTableID(tableID);
    shard->SetShardID(shardID);
    shard->SetFirstKey(firstKey);
    shard->SetLastKey(lastKey);
    shard->SetUseBloomFilter(useBloomFilter);
    shard->SetLogSegmentID(headLogSegment->GetLogSegmentID());
    shard->SetLogCommandID(headLogSegment->GetLogCommandID());

    memoChunk = new StorageMemoChunk;
    memoChunk->SetChunkID(nextChunkID++);
    memoChunk->SetUseBloomFilter(useBloomFilter);

    shard->PushMemoChunk(memoChunk);

    shards.Append(shard);
    
    WriteTOC();
    
    return true;
}

bool StorageEnvironment::DeleteShard(uint16_t /*contextID*/, uint64_t /*shardID*/)
{
    if (headLogSegment->HasUncommitted())
        return false;       // meta writes must occur in-between data writes (commits)

//    StorageShard* shard;
//
//    shard = GetShard(shardID);
//    if (shard == NULL)
//        return;
//
//    // TODO: decrease reference counts
//    shards.Delete(shard);
    return true;
}

bool StorageEnvironment::SplitShard(uint16_t contextID,  uint64_t shardID,
 uint64_t newShardID, ReadBuffer splitKey)
{
    StorageShard*           shard;
    StorageShard*           newShard;
    StorageMemoChunk*       memoChunk;
    StorageMemoChunk*       newMemoChunk;
    StorageChunk**          itChunk;
    StorageMemoKeyValue*    itKeyValue;
    StorageMemoKeyValue*    kv;
    
    if (headLogSegment->HasUncommitted())
        return false;       // meta writes must occur in-between data writes (commits)
    
    shard = GetShard(contextID, newShardID);
    if (shard != NULL)
        return false;       // exists

    shard = GetShard(contextID, shardID);
    if (shard == NULL)
        return false;       // does not exist

    newShard = new StorageShard;
    newShard->SetContextID(contextID);
    newShard->SetTableID(shard->GetTableID());
    newShard->SetShardID(newShardID);
    newShard->SetFirstKey(shard->GetFirstKey());
    newShard->SetLastKey(splitKey);
    newShard->SetUseBloomFilter(shard->UseBloomFilter());
    newShard->SetLogSegmentID(headLogSegment->GetLogSegmentID());
    newShard->SetLogCommandID(headLogSegment->GetLogCommandID());

    FOREACH(itChunk, shard->GetChunks())
        newShard->PushChunk(*itChunk);

    memoChunk = shard->GetMemoChunk();

    newMemoChunk = new StorageMemoChunk;
    newMemoChunk->chunkID = nextChunkID++;
    newMemoChunk->minLogSegmentID = memoChunk->minLogSegmentID;
    newMemoChunk->maxLogSegmentID = memoChunk->maxLogSegmentID;
    newMemoChunk->maxLogCommandID = memoChunk->maxLogCommandID;
    newMemoChunk->useBloomFilter = memoChunk->useBloomFilter;

    FOREACH(itKeyValue, memoChunk->keyValues)
    {
        if (newShard->RangeContains(itKeyValue->GetKey()))
        {
            kv = new StorageMemoKeyValue;
            if (itKeyValue->GetType() == STORAGE_KEYVALUE_TYPE_SET)
                newMemoChunk->Set(itKeyValue->GetKey(), itKeyValue->GetValue());
            else
                newMemoChunk->Delete(itKeyValue->GetKey());
            newMemoChunk->keyValues.Insert(kv);
        }
    }

    newShard->PushMemoChunk(newMemoChunk);

    shards.Append(newShard);

    shard->SetFirstKey(splitKey);
    
    WriteTOC();
    
    return true;
}

void StorageEnvironment::OnCommit()
{
    commitThreadActive = false;
    TryFinalizeLogSegment();
    TrySerializeChunks();
}

void StorageEnvironment::TryFinalizeLogSegment()
{
    Buffer tmp;

    if (headLogSegment->GetOffset() < config.logSegmentSize)
        return;

    headLogSegment->Close();

    logSegments.Append(headLogSegment);

    headLogSegment = new StorageLogSegment;
    tmp.Write(logPath);
    tmp.Appendf("log.%020U", nextLogSegmentID);
    headLogSegment->Open(tmp, nextLogSegmentID);
    nextLogSegmentID++;
}

void StorageEnvironment::TrySerializeChunks()
{
    Buffer                      tmp;
    StorageShard*               itShard;
    StorageMemoChunk*           memoChunk;
    StorageFileChunk*           fileChunk;
    StorageChunk**              itChunk;
    StorageJob*                 job;

    // look for memoChunks which have been serialized already
    FOREACH (itShard, shards)
    {
        for (itChunk = itShard->GetChunks().First(); itChunk != NULL; /* advanced in body */)
        {
            if ((*itChunk)->GetChunkState() == StorageChunk::Serialized)
            {
                memoChunk = (StorageMemoChunk*) (*itChunk);
                fileChunk = memoChunk->RemoveFileChunk();
                if (fileChunk == NULL)
                    goto Advance;
                itShard->OnChunkSerialized(memoChunk, fileChunk);
                Log_Message("Deleting MemoChunk...");
                job = new StorageDeleteMemoChunkJob(memoChunk);
                StartJob(asyncThread, job);
                //delete memoChunk;
                
                tmp.Write(chunkPath);
                tmp.Appendf("chunk.%020U", fileChunk->GetChunkID());
                fileChunk->SetFilename(tmp);
                fileChunks.Append(fileChunk);

                itChunk = itShard->GetChunks().First();
                continue;
            }
Advance:
            itChunk = itShard->GetChunks().Next(itChunk);
        }
    }

    if (serializerThreadActive)
        return;

    FOREACH (itShard, shards)
    {
        memoChunk = itShard->GetMemoChunk();
        if (
         memoChunk->GetSize() > config.chunkSize ||
         (
         headLogSegment->GetLogSegmentID() > STORAGE_MAX_UNBACKED_LOG_SEGMENT &&
         memoChunk->GetMinLogSegmentID() > 0 &&
         memoChunk->GetMinLogSegmentID() < (headLogSegment->GetLogSegmentID() - STORAGE_MAX_UNBACKED_LOG_SEGMENT
         )))
        {
            job = new StorageSerializeChunkJob(memoChunk, &onChunkSerialize);
            serializerThreadActive = true;
            StartJob(serializerThread, job);

            memoChunk = new StorageMemoChunk;
            memoChunk->SetChunkID(nextChunkID++);
            memoChunk->SetUseBloomFilter(itShard->UseBloomFilter());

            itShard->PushMemoChunk(memoChunk);

            return;
        }
    }
}

void StorageEnvironment::TryWriteChunks()
{
    StorageFileChunk*   it;
    StorageJob*         job;
    
    if (writerThreadActive)
        return;

    FOREACH (it, fileChunks)
    {
        if (it->GetChunkState() == StorageChunk::Unwritten)
        {
            job = new StorageWriteChunkJob(it, &onChunkWrite);
            asyncWriteChunkID = it->GetChunkID();
            writerThreadActive = true;
            StartJob(writerThread, job);
            return;
        }
    }
}

void StorageEnvironment::TryArchiveLogSegments()
{
    bool                archive;
    uint64_t            logSegmentID;
    StorageLogSegment*  logSegment;
    StorageShard*       itShard;
    StorageMemoChunk*   memoChunk;
    StorageChunk**      itChunk;
    StorageJob*         job;    
    
    if (archiverThreadActive || logSegments.GetLength() == 0)
        return;

    logSegment = logSegments.First();
    
    // a log segment cannot be archived
    // if there is a chunk which has not been written
    // which includes (may include) a write that is
    // stored in the log segment

    archive = true;
    logSegmentID = logSegment->GetLogSegmentID();
    Log_Message("Checking %U", logSegmentID);
    FOREACH (itShard, shards)
    {
        memoChunk = itShard->GetMemoChunk();
        if (memoChunk->GetMinLogSegmentID() > 0 && memoChunk->GetMinLogSegmentID() <= logSegmentID)
        {
            archive = false;
            Log_Message("Setting to false because of memoChunk's %U", memoChunk->GetMinLogSegmentID());

        }
        FOREACH (itChunk, itShard->GetChunks())
        {
            if ((*itChunk)->GetChunkState() <= StorageChunk::Unwritten)
            {
                assert((*itChunk)->GetMinLogSegmentID() > 0);
                if ((*itChunk)->GetMinLogSegmentID() <= logSegmentID)
                {
                    archive = false;
                    Log_Message("Setting to false because of chunk's %U", (*itChunk)->GetMinLogSegmentID());

                }
            }
        }
    }
    
    if (archive)
    {
        asyncLogSegmentID = logSegmentID;
        job = new StorageArchiveLogSegmentJob(this, logSegment, archiveScript, &onLogArchive);
        archiverThreadActive = true;
        StartJob(archiverThread, job);
        return;
    }
}

void StorageEnvironment::OnChunkSerialize()
{
    serializerThreadActive = false;
//    TrySerializeChunks(); don't do it here b/c chunks may contain uncommitted stuff
    TryWriteChunks();
}


void StorageEnvironment::OnChunkWrite()
{
    StorageFileChunk* it;
    
    writerThreadActive = false;

    FOREACH (it, fileChunks)
    {
        if (it->GetChunkID() == asyncWriteChunkID)
        {
            if (it->GetChunkState() != StorageChunk::Written)
            {
                Log_Message("Failed to write chunk %U to disk", it->GetChunkID());
                Log_Message("Possible causes: disk if full");
                Log_Message("Exiting...");
                STOP_FAIL(1);
            }
            // this was just written
            it->AddPagesToCache();
            break;
        }
    }

    WriteTOC();
    TryArchiveLogSegments();
    TryWriteChunks();
}

void StorageEnvironment::OnLogArchive()
{
    StorageLogSegment* it;
    
    archiverThreadActive = false;
 
    it = logSegments.First();
    
    assert(asyncLogSegmentID == it->GetLogSegmentID());
    
    logSegments.Delete(it);
    
    TryArchiveLogSegments();
}

bool StorageEnvironment::IsWriteActive()
{
    return commitThreadActive;
}

void StorageEnvironment::StartJob(ThreadPool* thread, StorageJob* job)
{
    MFunc<StorageJob, &StorageJob::Execute> callable(job);        
    thread->Execute(callable);
}

void StorageEnvironment::WriteTOC()
{
    StorageEnvironmentWriter writer;

    writer.Write(this);
}

StorageFileChunk* StorageEnvironment::GetFileChunk(uint64_t chunkID)
{
    StorageFileChunk*   it;

    FOREACH (it, fileChunks)
    {
        if (it->GetChunkID() == chunkID)
            return it;
    }
    
    return NULL;
}
