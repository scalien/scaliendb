#include "StorageEnvironment.h"
#include "System/Events/EventLoop.h"
#include "System/Events/Deferred.h"
#include "System/Stopwatch.h"
#include "System/FileSystem.h"
#include "System/Config.h"
#include "StorageChunkSerializer.h"
#include "StorageEnvironmentWriter.h"
#include "StorageRecovery.h"
#include "StoragePageCache.h"
#include "StorageAsyncGet.h"
#include "StorageAsyncList.h"

static inline int KeyCmp(const ReadBuffer& a, const ReadBuffer& b)
{
    return ReadBuffer::Cmp(a, b);
}

static inline const ReadBuffer Key(StorageMemoKeyValue* kv)
{
    return kv->GetKey();
}

static inline bool LessThan(const Buffer* a, const Buffer* b)
{
    return Buffer::Cmp(*a, *b) < 0;
}

StorageEnvironment::StorageEnvironment()
{
    commitThread = NULL;
    serializerThread = NULL;
    writerThread = NULL;
    mergerThread = NULL;
    asyncThread = NULL;
    asyncGetThread = NULL;

    onCommit = MFUNC(StorageEnvironment, OnCommit);
    onChunkSerialize = MFUNC(StorageEnvironment, OnChunkSerialize);
    onChunkWrite = MFUNC(StorageEnvironment, OnChunkWrite);
    onChunkMerge = MFUNC(StorageEnvironment, OnChunkMerge);
    onLogArchive = MFUNC(StorageEnvironment, OnLogArchive);
    onBackgroundTimer = MFUNC(StorageEnvironment, OnBackgroundTimer);
    backgroundTimer.SetCallable(onBackgroundTimer);
    
    nextChunkID = 1;
    nextLogSegmentID = 1;
    asyncLogSegmentID = 0;
    asyncWriteChunkID = 0;
    mergeShardID = 0;
    mergeContextID = 0;
    lastWriteTime = 0;
    yieldThreads = false;
    shuttingDown = false;
    writingTOC = false;
    numCursors = 0;
    serializeChunk = NULL;
    writeChunk = NULL;
    mergeChunkOut = NULL;
    mergeEnabled = true;
}

bool StorageEnvironment::Open(Buffer& envPath_)
{
    char            lastChar;
    Buffer          tmp;
    StorageRecovery recovery;

    config.Init();

    commitThread = ThreadPool::Create(1);
    commitThread->Start();
    commitThreadActive = false;
    
    serializerThread = ThreadPool::Create(1);
    serializerThread->Start();
    serializerThreadActive = false;

    writerThread = ThreadPool::Create(1);
    writerThread->Start();
    writerThreadActive = false;
    
    mergerThread = ThreadPool::Create(1);
    mergerThread->Start();
    mergerThreadActive = false;

    archiverThread = ThreadPool::Create(1);
    archiverThread->Start();
    archiverThreadActive = false;

    asyncThread = ThreadPool::Create(1);
    asyncThread->Start();

    asyncGetThread = ThreadPool::Create(1);
    asyncGetThread->Start();

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
    
    StoragePageCache::Init(config);
    
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
    headLogSegment->Open(tmp, nextLogSegmentID, config.syncGranularity);
    nextLogSegmentID++;

    backgroundTimer.SetDelay(1000 * configFile.GetIntValue("database.backgroundTimerDelay",
     STORAGE_DEFAULT_BACKGROUND_TIMER_DELAY));

    EventLoop::Add(&backgroundTimer);

    maxUnbackedLogSegments = configFile.GetIntValue("database.maxUnbackedLogSegments",
     STORAGE_DEFAULT_MAX_UNBACKED_LOG_SEGMENT);

    return true;
}

void StorageEnvironment::Close()
{
    shuttingDown = true;

    asyncGetThread->Stop();
    asyncThread->Stop();

    commitThread->Stop();
    commitThreadActive = false;
    serializerThread->Stop();
    serializerThreadActive = false;
    writerThread->Stop();
    writerThreadActive = false;
    mergerThread->Stop();
    mergerThreadActive = false;
    archiverThread->Stop();
    archiverThreadActive = false;

    delete commitThread;
    delete serializerThread;
    delete writerThread;
    delete mergerThread;
    delete archiverThread;
    delete asyncThread;
    delete asyncGetThread;
    
    shards.DeleteList();
    delete headLogSegment;
    logSegments.DeleteList();

    fileChunks.DeleteList();
}

void StorageEnvironment::SetYieldThreads(bool yieldThreads_)
{
    yieldThreads = yieldThreads_;
}

void StorageEnvironment::SetMergeEnabled(bool mergeEnabled_)
{
    mergeEnabled = mergeEnabled_;
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

bool StorageEnvironment::ShardExists(uint16_t contextID, uint64_t shardID)
{
    StorageShard* it;

    FOREACH (it, shards)
    {
        if (it->GetContextID() == contextID && it->GetShardID() == shardID)
            return true;
    }

    return false;
}

void StorageEnvironment::GetShardIDs(uint64_t contextID, ShardIDList& shardIDs)
{
    uint64_t            shardID;
    StorageShard*       itShard;
    
    FOREACH(itShard, shards)
    {
        if (itShard->GetContextID() != contextID)
            continue;
        
        shardID = itShard->GetShardID();
        shardIDs.Append(shardID);
    }
}

void StorageEnvironment::GetShardIDs(uint64_t contextID, uint64_t tableID, ShardIDList& shardIDs)
{
    uint64_t            shardID;
    StorageShard*       itShard;
    
    FOREACH(itShard, shards)
    {
        if (itShard->GetContextID() != contextID || itShard->GetTableID() != tableID)
            continue;
        
        shardID = itShard->GetShardID();
        shardIDs.Append(shardID);
    }
}

bool StorageEnvironment::Get(uint16_t contextID, uint64_t shardID, ReadBuffer key, ReadBuffer& value)
{
    StorageShard*       shard;
    StorageChunk*       chunk;
    StorageChunk**      itChunk;
    StorageKeyValue*    kv;

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

bool StorageEnvironment::TryNonblockingGet(uint16_t contextID, uint64_t shardID, StorageAsyncGet* asyncGet)
{
    StorageShard*       shard;
    StorageChunk*       chunk;
    StorageKeyValue*    kv;
    Deferred            deferred(asyncGet->onComplete);

    asyncGet->completed = true;
    asyncGet->ret = false;
    shard = GetShard(contextID, shardID);
    if (shard == NULL)
        return true;
        
    if (!shard->RangeContains(asyncGet->key))
        return true;

    chunk = shard->GetMemoChunk();
    kv = chunk->Get(asyncGet->key);
    if (kv != NULL)
    {
        if (kv->GetType() == STORAGE_KEYVALUE_TYPE_SET)
        {
            asyncGet->ret = true;
            asyncGet->value = kv->GetValue();
            return true;
        }
        else if (kv->GetType() == STORAGE_KEYVALUE_TYPE_DELETE)
            return true;
        else
            ASSERT_FAIL();
    }

    if (shard->GetChunks().GetLength() == 0)
        return true;
    
    deferred.Unset();
    return false;
}

void StorageEnvironment::AsyncGet(uint16_t contextID, uint64_t shardID, StorageAsyncGet* asyncGet)
{
    StorageShard*       shard;
    StorageChunk*       chunk;
    StorageKeyValue*    kv;
    Deferred            deferred(asyncGet->onComplete);

    asyncGet->completed = true;
    asyncGet->ret = false;
        
    shard = GetShard(contextID, shardID);
    if (shard == NULL)
        return;

    if (!asyncGet->skipMemoChunk)
    {
        if (!shard->RangeContains(asyncGet->key))
            return;

        chunk = shard->GetMemoChunk();
        kv = chunk->Get(asyncGet->key);
        if (kv != NULL)
        {
            if (kv->GetType() == STORAGE_KEYVALUE_TYPE_SET)
            {
                asyncGet->ret = true;
                asyncGet->value = kv->GetValue();
                return;
            }
            else if (kv->GetType() == STORAGE_KEYVALUE_TYPE_DELETE)
                return;
            else
                ASSERT_FAIL();
        }

        if (shard->GetChunks().GetLength() == 0)
            return;
    }
    
    deferred.Unset();
    asyncGet->completed = false;
    asyncGet->shard = shard;
    asyncGet->itChunk = shard->GetChunks().Last();
    asyncGet->lastLoadedPage = NULL;
    asyncGet->stage = StorageAsyncGet::START;
    asyncGet->threadPool = asyncGetThread;
    asyncGet->ExecuteAsyncGet();
}

void StorageEnvironment::AsyncList(uint16_t contextID, uint64_t shardID, StorageAsyncList* asyncList)
{
    StorageShard*       shard;
    Deferred            deferred(asyncList->onComplete);

    asyncList->completed = true;
    asyncList->ret = false;
    
    shard = GetShard(contextID, shardID);
    if (shard == NULL)
        return;
        
    if (!shard->RangeContains(asyncList->shardFirstKey))
        return;

    numCursors++;
    
    deferred.Unset();
    asyncList->completed = false;
    asyncList->env = this;
    asyncList->shard = shard;
    asyncList->stage = StorageAsyncList::START;
    asyncList->threadPool = asyncThread;
    asyncList->ExecuteAsyncList();
}

bool StorageEnvironment::Set(uint16_t contextID, uint64_t shardID, ReadBuffer key, ReadBuffer value)
{
    int32_t             logCommandID;
    StorageShard*       shard;
    StorageMemoChunk*   memoChunk;
    
    if (commitThreadActive)
    {
        ASSERT_FAIL();
        return false;
    }
    
    shard = GetShard(contextID, shardID);
    if (shard == NULL)
        return false;

    logCommandID = headLogSegment->AppendSet(contextID, shardID, key, value);
    if (logCommandID < 0)
    {
        ASSERT_FAIL();
        return false;
    }

    memoChunk = shard->GetMemoChunk();
    ASSERT(memoChunk != NULL);
    
    if (shard->IsLogStorage())
    {
        while (memoChunk->GetSize() > config.chunkSize)
            memoChunk->RemoveFirst();
    }
    
    if (!memoChunk->Set(key, value))
    {
        headLogSegment->Undo();
        return false;
    }
    memoChunk->RegisterLogCommand(headLogSegment->GetLogSegmentID(), logCommandID);

//    haveUncommitedWrites = true;

    return true;
}

bool StorageEnvironment::Delete(uint16_t contextID, uint64_t shardID, ReadBuffer key)
{
    int32_t             logCommandID;
    StorageShard*       shard;
    StorageMemoChunk*   memoChunk;

    if (commitThreadActive)
    {
        ASSERT_FAIL();
        return false;
    }
    
    shard = GetShard(contextID, shardID);
    if (shard == NULL)
        return false;

    if (!shard->IsLogStorage())
    {
        logCommandID = headLogSegment->AppendDelete(contextID, shardID, key);
        if (logCommandID < 0)
        {
            ASSERT_FAIL();
            return false;
        }

        memoChunk = shard->GetMemoChunk();
        ASSERT(memoChunk != NULL);
        if (!memoChunk->Delete(key))
        {
            headLogSegment->Undo();
            return false;
        }
        memoChunk->RegisterLogCommand(headLogSegment->GetLogSegmentID(), logCommandID);

//        haveUncommitedWrites = true;
    }
    else
    {
        ASSERT_FAIL();
    }
    
    return true;
}

StorageBulkCursor* StorageEnvironment::GetBulkCursor(uint16_t contextID, uint64_t shardID)
{
    StorageBulkCursor*  bc;

    bc = new StorageBulkCursor();

    bc->SetEnvironment(this);
    bc->SetShard(contextID, shardID);
    
    numCursors++;
    
    return bc;
}

StorageAsyncBulkCursor* StorageEnvironment::GetAsyncBulkCursor(uint16_t contextID, uint64_t shardID,
 Callable onResult)
{
    StorageAsyncBulkCursor*  abc;

    abc = new StorageAsyncBulkCursor();

    abc->SetEnvironment(this);
    abc->SetShard(contextID, shardID);
    abc->SetThreadPool(asyncThread);
    abc->SetOnComplete(onResult);
    
    numCursors++;
    
    return abc;
}

void StorageEnvironment::DecreaseNumCursors()
{
    ASSERT(numCursors > 0);
    numCursors--;
}

uint64_t StorageEnvironment::GetSize(uint16_t contextID, uint64_t shardID)
{
    uint64_t            size;
    StorageShard*       shard;
    StorageFileChunk*   chunk;
    StorageChunk**      itChunk;
    ReadBuffer          firstKey;
    ReadBuffer          lastKey;

    shard = GetShard(contextID, shardID);
    if (!shard)
        return 0;

    size = shard->GetMemoChunk()->GetSize();
    
    FOREACH (itChunk, shard->GetChunks())
    {
        if ((*itChunk)->GetChunkState() != StorageChunk::Written)
        {
            size += (*itChunk)->GetSize();
            continue;
        }
        
        chunk = (StorageFileChunk*) *itChunk;
        firstKey = chunk->GetFirstKey();
        lastKey = chunk->GetLastKey();
        
        if (firstKey.GetLength() > 0)
        {
            if (!shard->RangeContains(firstKey))
            {
                size += chunk->GetPartialSize(shard->GetFirstKey(), shard->GetLastKey());
                continue;
            }
        }

        if (lastKey.GetLength() > 0)
        {
            if (!shard->RangeContains(lastKey))
            {
                size += chunk->GetPartialSize(shard->GetFirstKey(), shard->GetLastKey());
                continue;
            }
        }

        size += chunk->GetSize();
    }
    
    return size;
}

ReadBuffer StorageEnvironment::GetMidpoint(uint16_t contextID, uint64_t shardID)
{
    unsigned        numChunks, i;
    StorageShard*   shard;
    StorageChunk**  itChunk;
    ReadBuffer      firstKey;
    ReadBuffer      lastKey;

    shard = GetShard(contextID, shardID);
    if (!shard)
        return ReadBuffer();
    
    numChunks = shard->GetChunks().GetLength();
    
    i = 0;
    FOREACH(itChunk, shard->GetChunks())
    {
        i++;

        if (i >= ((numChunks + 1) / 2))
        {
            firstKey = (*itChunk)->GetFirstKey();
            lastKey = (*itChunk)->GetLastKey();
            
            // skip non-splitable chunks
            if (firstKey.GetLength() > 0)
            {
                if (!shard->RangeContains(firstKey))
                    continue;
            }

            if (lastKey.GetLength() > 0)
            {
                if (!shard->RangeContains(lastKey))
                    continue;
            }
            
            return (*itChunk)->GetMidpoint();
        }
    }
    
    return shard->GetMemoChunk()->GetMidpoint();
}

bool StorageEnvironment::IsSplitable(uint16_t contextID, uint64_t shardID)
{
    StorageShard*   shard;
    StorageChunk**  itChunk;
    ReadBuffer      firstKey;
    ReadBuffer      lastKey;

    shard = GetShard(contextID, shardID);
    if (!shard)
        return 0;
    
    FOREACH(itChunk, shard->GetChunks())
    {
        firstKey = (*itChunk)->GetFirstKey();
        lastKey = (*itChunk)->GetLastKey();
        
        if (firstKey.GetLength() > 0)
        {
            if (!shard->RangeContains(firstKey))
                return false;
        }

        if (lastKey.GetLength() > 0)
        {
            if (!shard->RangeContains(lastKey))
                return false;
        }
    }
    
    return true;
}

bool StorageEnvironment::Commit(Callable& onCommit_)
{
    StorageJob*     job;

    
    onCommitCallback = onCommit_;

    if (commitThreadActive)
    {
        ASSERT_FAIL();
        return false;
    }

    job = new StorageCommitJob(headLogSegment, &onCommit);
    commitThreadActive = true;
    StartJob(commitThread, job);
    
    return true;
}

bool StorageEnvironment::Commit()
{
    if (commitThreadActive)
        return false;

    headLogSegment->Commit();
    OnCommit();

    return true;
}

bool StorageEnvironment::GetCommitStatus()
{
    return headLogSegment->GetCommitStatus();
}

bool StorageEnvironment::IsCommiting()
{
    return commitThreadActive;
}

bool StorageEnvironment::IsShuttingDown()
{
    return shuttingDown;
}

void StorageEnvironment::PrintState(uint16_t contextID, Buffer& buffer)
{
#define MAKE_PRINTABLE(a) \
printable.Write(a); if (!printable.IsAsciiPrintable()) { printable.ToHexadecimal(); }

    bool                isSplitable;
    ReadBuffer          firstKey;
    ReadBuffer          lastKey;
    ReadBuffer          midpoint;
    StorageShard*       shard;
    StorageChunk**      itChunk;
    StorageMemoChunk*   memoChunk;
    Buffer              printable;
    
    buffer.Clear();
    
    FOREACH(shard, shards)
    {
        if (shard->GetContextID() != contextID)
            continue;
        
        firstKey = shard->GetFirstKey();
        lastKey = shard->GetLastKey();
        midpoint = GetMidpoint(contextID, shard->GetShardID());
        isSplitable = IsSplitable(contextID, shard->GetShardID());
        
        buffer.Appendf("- shard %U (tableID = %U) \n", shard->GetShardID(), shard->GetTableID());
        buffer.Appendf("   size: %s\n", HUMAN_BYTES(GetSize(contextID, shard->GetShardID())));
        buffer.Appendf("   isSplitable: %b\n", isSplitable);

        MAKE_PRINTABLE(firstKey);
        if (printable.GetLength() == 0)
            buffer.Appendf("   firstKey: (empty)\n");
        else
            buffer.Appendf("   firstKey: %B\n", &printable);

        MAKE_PRINTABLE(lastKey);
        if (printable.GetLength() == 0)
            buffer.Appendf("   lastKey: (empty)\n");
        else
            buffer.Appendf("   lastKey: %B\n", &printable);

        MAKE_PRINTABLE(midpoint);
        if (printable.GetLength() == 0)
            buffer.Appendf("   midpoint: (empty)\n");
        else
            buffer.Appendf("   midpoint: %B\n", &printable);
        buffer.Appendf("   logSegmentID: %U\n", shard->GetLogSegmentID());
        buffer.Appendf("   logCommandID: %U\n", shard->GetLogCommandID());
        
        memoChunk = shard->GetMemoChunk();
        firstKey = memoChunk->GetFirstKey();
        lastKey = memoChunk->GetLastKey();
        midpoint = memoChunk->GetMidpoint();
        buffer.Appendf("    * memo chunk %U\n", memoChunk->GetChunkID());
        buffer.Appendf("       state: %d {0=Tree, 1=Serialized, 2=Unwritten, 3=Written}\n",
         memoChunk->GetChunkState());
        buffer.Appendf("       size: %s\n", HUMAN_BYTES(memoChunk->GetSize()));
        buffer.Appendf("       count: %U\n", memoChunk->keyValues.GetCount());
        MAKE_PRINTABLE(firstKey);
        buffer.Appendf("       firstKey: %B\n", &printable);
        MAKE_PRINTABLE(lastKey);
        buffer.Appendf("       lastKey: %B\n", &printable);
        MAKE_PRINTABLE(midpoint);
        buffer.Appendf("       midpoint: %B\n", &printable);
        buffer.Appendf("       minLogSegmentID: %U\n", memoChunk->GetMinLogSegmentID());
        buffer.Appendf("       maxLogSegmentID: %U\n", memoChunk->GetMaxLogSegmentID());
        buffer.Appendf("       maxLogCommandID: %U\n", memoChunk->GetMaxLogCommandID());

        FOREACH(itChunk, shard->GetChunks())
        {
            firstKey = (*itChunk)->GetFirstKey();
            lastKey = (*itChunk)->GetLastKey();
            midpoint = (*itChunk)->GetMidpoint();
            buffer.Appendf("    * chunk %U\n", (*itChunk)->GetChunkID());
            buffer.Appendf("       state: %d {0=Tree, 1=Serialized, 2=Unwritten, 3=Written}\n",
             (*itChunk)->GetChunkState());
            buffer.Appendf("       size: %s\n", HUMAN_BYTES((*itChunk)->GetSize()));
            MAKE_PRINTABLE(firstKey);
            buffer.Appendf("       firstKey: %B\n", &printable);
            MAKE_PRINTABLE(lastKey);
            buffer.Appendf("       lastKey: %B\n", &printable);
            MAKE_PRINTABLE(midpoint);
            buffer.Appendf("       midpoint: %B\n", &printable);
            buffer.Appendf("       minLogSegmentID: %U\n", (*itChunk)->GetMinLogSegmentID());
            buffer.Appendf("       maxLogSegmentID: %U\n", (*itChunk)->GetMaxLogSegmentID());
            buffer.Appendf("       maxLogCommandID: %U\n", (*itChunk)->GetMaxLogCommandID());
        }

        buffer.Appendf("\n");
    }
}

StorageConfig& StorageEnvironment::GetConfig()
{
    return config;
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
 ReadBuffer firstKey, ReadBuffer lastKey, bool useBloomFilter, bool isLogStorage)
{
    StorageShard*       shard;
    StorageMemoChunk*   memoChunk;

// TODO
//    if (headLogSegment->HasUncommitted())
//        return false;       // meta writes must occur in-between data writes (commits)
    
    shard = GetShard(contextID, shardID);
    if (shard != NULL)
        return false;       // already exists

    Log_Debug("Creating shard %u/%U", contextID, shardID);

    shard = new StorageShard;
    shard->SetContextID(contextID);
    shard->SetTableID(tableID);
    shard->SetShardID(shardID);
    shard->SetFirstKey(firstKey);
    shard->SetLastKey(lastKey);
    shard->SetUseBloomFilter(useBloomFilter);
    shard->SetLogStorage(isLogStorage);
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

bool StorageEnvironment::DeleteShard(uint16_t contextID, uint64_t shardID)
{
    bool                    found;
    StorageShard*           shard;
    StorageShard*           itShard;
    StorageChunk**          chunk;
    StorageChunk**          itChunk;
    StorageMemoChunk*       memoChunk;
    StorageFileChunk*       fileChunk;
    StorageJob*             job;

// TODO
//    if (headLogSegment->HasUncommitted())
//        return false;       // meta writes must occur in-between data writes (commits)

    shard = GetShard(contextID, shardID);
    if (shard == NULL)
        return true;        // does not exists

    Log_Debug("Deleting shard %u/%U", contextID, shardID);

    // delete memoChunk
    if (shard->GetMemoChunk() != NULL)
    {
        memoChunk = shard->GetMemoChunk();
        Log_Debug("Deleting MemoChunk...");
        job = new StorageDeleteMemoChunkJob(memoChunk);
        StartJob(asyncThread, job);
        shard->memoChunk = NULL; // TODO: private hack
    }

    FOREACH (itShard, shards)
    {
        Log_Debug("Listing chunks for shard %u/%U", itShard->GetContextID(), itShard->GetShardID());
        FOREACH (itChunk, itShard->GetChunks())
            Log_Debug(" - Chunk %U (state = %u)", (*itChunk)->GetChunkID(), (*itChunk)->GetChunkState());
    }

    for (chunk = shard->GetChunks().First(); chunk != NULL; /* advanced in body*/ )
    {
        found = false;
        FOREACH (itShard, shards)
        {
            if (itShard->GetShardID() == shard->GetShardID())
                continue;
            FOREACH (itChunk, itShard->GetChunks())
            {
                if ((*chunk)->GetChunkID() == (*itChunk)->GetChunkID())
                {
                    found = true;
                    break;
                }
            }
            if (found)
                break;
        }
        
        if (found)
            goto Advance;

        if ((*chunk)->GetChunkState() <= StorageChunk::Serialized)
        {
            memoChunk = (StorageMemoChunk*) *chunk;
            if (serializeChunk == *chunk)
            {
                memoChunk->deleted = true;
            }
            else
            {
                job = new StorageDeleteMemoChunkJob(memoChunk);
                StartJob(asyncThread, job);
            }
        }
        else
        {
            fileChunk = (StorageFileChunk*) *chunk;
            fileChunks.Remove(fileChunk);

            StorageFileChunk**  mergeChunk;
            FOREACH (mergeChunk, mergeChunks)
            {
                if (fileChunk == *mergeChunk)
                {
                    fileChunk->deleted = true;
                    goto Advance;
                }
            }
            
            if (fileChunk == writeChunk)
            {
                fileChunk->deleted = true;
                goto Advance;
            }

            Log_Debug("Removing chunk %U from caches", fileChunk->GetChunkID());
            fileChunk->RemovePagesFromCache();
            job = new StorageDeleteFileChunkJob(fileChunk);
            StartJob(asyncThread, job);                    
        }

        Advance:
        chunk = shard->GetChunks().Remove(chunk);
    }
    
    if (mergeContextID == contextID && mergeShardID == shardID)
        mergeChunkOut->deleted = true;

    shards.Remove(shard);
    delete shard;
    
    WriteTOC(); // TODO: this is not completely right
    
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
        Commit();

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
    newShard->SetFirstKey(splitKey);
    newShard->SetLastKey(shard->GetLastKey());
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

    shard->SetLastKey(splitKey);
    
    WriteTOC();
    
    return true;
}

void StorageEnvironment::OnCommit()
{
    bool            asyncCommit;
    StorageShard*   shard;
    
    asyncCommit = commitThreadActive;
    
    commitThreadActive = false;
//    haveUncommitedWrites = false;
    
    FOREACH(shard, shards)
        shard->GetMemoChunk()->haveUncommitedWrites = false;

    TryFinalizeLogSegment();
    TrySerializeChunks();
    
    if (asyncCommit)
        Call(onCommitCallback);
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
    headLogSegment->Open(tmp, nextLogSegmentID, config.syncGranularity);
    nextLogSegmentID++;
}

void StorageEnvironment::TrySerializeChunks()
{
    Buffer                      tmp;
    StorageShard*               itShard;
    StorageMemoChunk*           memoChunk;
    StorageJob*                 job;

    if (serializerThreadActive)
        return;

//    if (haveUncommitedWrites)
//        return;

    FOREACH (itShard, shards)
    {
        if (itShard->IsLogStorage())
            continue; // never serialize log storage shards
        
        memoChunk = itShard->GetMemoChunk();
        
        if (memoChunk->haveUncommitedWrites)
            continue;
        
        if (
         memoChunk->GetSize() > config.chunkSize ||
         (
         headLogSegment->GetLogSegmentID() > maxUnbackedLogSegments &&
         memoChunk->GetMinLogSegmentID() > 0 &&
         memoChunk->GetMinLogSegmentID() < (headLogSegment->GetLogSegmentID() - maxUnbackedLogSegments
         )))
        {
            serializeChunk = memoChunk;
            job = new StorageSerializeChunkJob(this, memoChunk, &onChunkSerialize);
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
            job = new StorageWriteChunkJob(this, it, &onChunkWrite);
            asyncWriteChunkID = it->GetChunkID();
            writeChunk = it;
            writerThreadActive = true;
            StartJob(writerThread, job);
            return;
        }
    }
}

void StorageEnvironment::TryMergeChunks()
{
    Buffer                  tmp;
    StorageShard*           itShard;
    StorageChunk**          itChunk;
    StorageFileChunk*       fileChunk;
    StorageFileChunk**      itFileChunk;
    StorageJob*             job;
    List<Buffer*>           filenames;
    Buffer*                 filename;
    uint64_t                oldSize;
    uint64_t                youngSize;
    uint64_t                totalSize;

    if (!mergeEnabled)
        return;

    if (mergerThreadActive)
        return;

    if (numCursors > 0)
        return;

    FOREACH (itShard, shards)
    {
        if (itShard->IsLogStorage())
            continue;
        
        totalSize = 0;
        FOREACH (itChunk, itShard->GetChunks())
        {
            if ((*itChunk)->GetChunkState() != StorageChunk::Written)
                continue;
            fileChunk = (StorageFileChunk*) *itChunk;
            mergeChunks.Append(fileChunk);
            totalSize += fileChunk->GetSize();
        }

        while (mergeChunks.GetLength() >= 3)
        {
            itFileChunk = mergeChunks.First();
            oldSize = (*itFileChunk)->GetSize();
            youngSize = totalSize - oldSize;
            if (oldSize > youngSize * 1.1)
            {
                mergeChunks.Remove(mergeChunks.First());
                totalSize -= oldSize;
            }
            else break;
        }
        
        if (mergeChunks.GetLength() < 3)
        {
            mergeChunks.Clear();
            continue;   // on next shard
        }
        
        FOREACH (itFileChunk, mergeChunks)
        {
            filename = &(*itFileChunk)->GetFilename();
            filenames.Add(filename);
        }

        mergeChunkOut = new StorageFileChunk;
        mergeChunkOut->headerPage.SetChunkID(nextChunkID++);
        mergeChunkOut->headerPage.SetUseBloomFilter(itShard->UseBloomFilter());
        tmp.Write(chunkPath);
        tmp.Appendf("chunk.%020U", mergeChunkOut->GetChunkID());
        mergeChunkOut->SetFilename(ReadBuffer(tmp));
        job = new StorageMergeChunkJob(
         this,
         filenames,
         mergeChunkOut, itShard->GetFirstKey(), itShard->GetLastKey(), &onChunkMerge);
        mergeShardID = itShard->GetShardID();
        mergeContextID = itShard->GetContextID();
        mergerThreadActive = true;
        StartJob(mergerThread, job);
        return;
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
    FOREACH (itShard, shards)
    {
        if (itShard->IsLogStorage())
            continue; // log storage shards never hinder log segment archival
        memoChunk = itShard->GetMemoChunk();
        if (memoChunk->GetMinLogSegmentID() > 0 && memoChunk->GetMinLogSegmentID() <= logSegmentID)
            archive = false;
        FOREACH (itChunk, itShard->GetChunks())
        {
            if ((*itChunk)->GetChunkState() <= StorageChunk::Unwritten)
            {
                ASSERT((*itChunk)->GetMinLogSegmentID() > 0);
                if ((*itChunk)->GetMinLogSegmentID() <= logSegmentID)
                    archive = false;
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
    Buffer                      tmp;
    StorageShard*               itShard;
    StorageMemoChunk*           memoChunk;
    StorageFileChunk*           fileChunk;
    StorageChunk**              itChunk;
    StorageJob*                 job;

    ASSERT(serializerThreadReturnCode);
    serializeChunk->serialized = true;
    
    if (serializeChunk->deleted)
    {
        job = new StorageDeleteMemoChunkJob(serializeChunk);
        StartJob(asyncThread, job);
    }
    else
    {
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
                    OnChunkSerialized(memoChunk, fileChunk);
                    Log_Debug("Deleting MemoChunk...");
                    job = new StorageDeleteMemoChunkJob(memoChunk);
                    StartJob(asyncThread, job);
                    
                    tmp.Write(chunkPath);
                    tmp.Appendf("chunk.%020U", fileChunk->GetChunkID());
                    fileChunk->SetFilename(ReadBuffer(tmp));
                    fileChunks.Append(fileChunk);

                    itChunk = itShard->GetChunks().First();
                    continue;
                }
Advance:
                itChunk = itShard->GetChunks().Next(itChunk);
            }
        }
    }
    
    serializeChunk = NULL;
    serializerThreadActive = false;
    TrySerializeChunks();
    TryWriteChunks();
}


void StorageEnvironment::OnChunkWrite()
{
    StorageFileChunk*   it;
    StorageJob*         job;

    ASSERT(writerThreadReturnCode);
    writeChunk->written = true;
    
    if (writeChunk->deleted)
    {
        job = new StorageDeleteFileChunkJob(writeChunk);
        StartJob(asyncThread, job);                    
    }
    
    writerThreadActive = false;
    writeChunk = NULL;
    lastWriteTime = EventLoop::Now();

    FOREACH (it, fileChunks)
    {
        if (it->GetChunkID() == asyncWriteChunkID)
        {
            if (it->GetChunkState() != StorageChunk::Written)
            {
                Log_Message("Failed to write chunk %U to disk", it->GetChunkID());
                Log_Message("Possible causes: disk is full");
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
    TryMergeChunks();
}

void StorageEnvironment::OnChunkMerge()
{
    StorageShard*       itShard;
    StorageJob*         job;
    StorageChunk**      itChunk;
    StorageFileChunk**  itFileChunk;
    StorageFileChunk*   fileChunk;
    StorageChunk*       chunk;
    bool                deleteOut;

    mergerThreadActive = false;
    
    if (!mergeChunkOut->written)
    {
        deleteOut = true;
        goto Delete;
    }

    itShard = GetShard(mergeContextID, mergeShardID);

    deleteOut = false;
    if (itShard == NULL)
    {
        // shard has been deleted
        deleteOut = true;
        goto Delete;
    }

    ASSERT(mergeChunkOut->written);
    
    // delete the merged chunks from the shard
    FOREACH (itFileChunk, mergeChunks)
    {
        chunk = (StorageChunk*) *itFileChunk;
        itShard->GetChunks().Remove(chunk);
    }

    itShard->GetChunks().Add(mergeChunkOut);

    // TODO: async
    WriteTOC();

    // check if the merged chunks belong to other shards
    FOREACH (itShard, shards)
    {
        FOREACH (itChunk, itShard->GetChunks())
        {
            FOREACH (itFileChunk, mergeChunks)
            {
                if (*itChunk == *itFileChunk)
                {
                    mergeChunks.Remove(*itFileChunk);
                    break;  // foreach mergeChunks
                }
            }
        }
    }

Delete:

    // enqueue the remaining merged chunks for deleting
    FOREACH_FIRST (itFileChunk, mergeChunks)
    {
        fileChunk = *itFileChunk;

        if (mergeChunkOut->written || fileChunk->deleted)
        {
            fileChunk->RemovePagesFromCache();
            if (!fileChunk->deleted)
                fileChunks.Remove(fileChunk);

            job = new StorageDeleteFileChunkJob(fileChunk);
            StartJob(writerThread, job);
        }
        mergeChunks.Remove(*itFileChunk);
    }

    if (deleteOut)
    {
        // in case the shard was deleted while merging
        job = new StorageDeleteFileChunkJob(mergeChunkOut);
        StartJob(writerThread, job);
    }
    else
    {
        fileChunks.Append(mergeChunkOut);
        mergeChunkOut->AddPagesToCache();
    }
    
    mergeChunkOut = NULL;
    mergeContextID = 0;
    mergeShardID = 0;
    
    TryMergeChunks();
}

void StorageEnvironment::OnLogArchive()
{
    StorageLogSegment* it;
    
    archiverThreadActive = false;
 
    it = logSegments.First();
    
    ASSERT(asyncLogSegmentID == it->GetLogSegmentID());
    
    logSegments.Delete(it);
    
    TryArchiveLogSegments();
}

void StorageEnvironment::OnBackgroundTimer()
{
    TrySerializeChunks();
    TryWriteChunks();
    TryMergeChunks();
    TryArchiveLogSegments();
    
    EventLoop::Add(&backgroundTimer);
}

void StorageEnvironment::StartJob(ThreadPool* thread, StorageJob* job)
{
    MFunc<StorageJob, &StorageJob::Execute> callable(job);        
    thread->Execute(callable);
}

void StorageEnvironment::WriteTOC()
{
    StorageEnvironmentWriter    writer;

    Log_Debug("WriteTOC started");

    writingTOC = true;
    writer.Write(this);
    writingTOC = false;
    
    Log_Debug("WriteTOC finished");
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

void StorageEnvironment::OnChunkSerialized(StorageMemoChunk* memoChunk, StorageFileChunk* fileChunk)
{
    StorageShard*   shard;
    StorageChunk**  chunk;
    
    FOREACH(shard, shards)
    {
        for (chunk = shard->GetChunks().First(); chunk != NULL; chunk = shard->GetChunks().Next(chunk))
        {
            if (*chunk == memoChunk)
            {
                shard->OnChunkSerialized(memoChunk, fileChunk);
                break;
            }
        }
    }
}
