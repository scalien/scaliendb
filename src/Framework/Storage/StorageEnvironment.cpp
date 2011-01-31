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

static inline int KeyCmp(const ReadBuffer& a, const ReadBuffer& b)
{
    return ReadBuffer::Cmp(a, b);
}

static inline const ReadBuffer Key(StorageMemoKeyValue* kv)
{
    return kv->GetKey();
}

static bool LessThan(const StorageAsyncGet* a, const StorageAsyncGet* b)
{
    return ReadBuffer::Cmp(a->key, b->key);
}

// This function is executed in the main thread
void StorageAsyncGet::ExecuteAsyncGet()
{
    StorageFileChunk*   fileChunk;

    fileChunk = (StorageFileChunk*) (*itChunk);
    if (stage == BLOOM_PAGE)
    {
        if (lastLoadedPage != NULL)
        {
            assert(fileChunk->bloomPage == NULL);
            fileChunk->bloomPage = (StorageBloomPage*) lastLoadedPage;
            fileChunk->isBloomPageLoading = false;
        }
    }
    else if (stage == INDEX_PAGE)
    {
        if (lastLoadedPage != NULL)
        {
            assert(fileChunk->indexPage == NULL);
            fileChunk->indexPage = (StorageIndexPage*) lastLoadedPage;
            fileChunk->AllocateDataPageArray();
            fileChunk->isIndexPageLoading = false;
        }
    }
    else if (stage == DATA_PAGE)
    {
        if (lastLoadedPage != NULL)
        {
            // TODO: FIXME:
//            assert(fileChunk->dataPages[index] == NULL);
            if (fileChunk->dataPages[index] == NULL)
            {
                fileChunk->dataPages[index] = (StorageDataPage*) lastLoadedPage;
                StoragePageCache::AddPage(lastLoadedPage);
                lastLoadedPage = NULL;
            }
            else
            {
                delete lastLoadedPage;
                lastLoadedPage = NULL;
            }
        }
    }

    if (lastLoadedPage)
    {
        StoragePageCache::AddPage(lastLoadedPage);
        lastLoadedPage = NULL;
    }
    
    stage = START;
    while (itChunk != NULL)
    {
        completed = false;
        (*itChunk)->AsyncGet(this);
        if (completed && ret)
            break;  // found

        if (!completed)
            return; // needs async loading

        itChunk = shard->GetChunks().Prev(itChunk);
    }

    if (itChunk == NULL)
        completed = true;

    if (completed)
        OnComplete();

    // don't put code here, because OnComplete deletes the object
}

// This function is executed in the main thread
void StorageAsyncGet::OnComplete()
{
    Call(onComplete);
    delete this;
}

// This function is executed in the threadPool
void StorageAsyncGet::AsyncLoadPage()
{
    Callable            asyncGet;
    StorageFileChunk*   fileChunk;
    
    fileChunk = (StorageFileChunk*) (*itChunk);
    if (stage == BLOOM_PAGE)
        lastLoadedPage = fileChunk->AsyncLoadBloomPage();
    else if (stage == INDEX_PAGE)
        lastLoadedPage = fileChunk->AsyncLoadIndexPage();
    else if (stage == DATA_PAGE)
        lastLoadedPage = fileChunk->AsyncLoadDataPage(index, offset);
    
    asyncGet = MFUNC(StorageAsyncGet, ExecuteAsyncGet);
    IOProcessor::Complete(&asyncGet);
}


StorageEnvironment::StorageEnvironment()
{
    commitThread = NULL;
    serializerThread = NULL;
    writerThread = NULL;
    asyncThread = NULL;
    asyncGetThread = NULL;

    onCommit = MFUNC(StorageEnvironment, OnCommit);
    onChunkSerialize = MFUNC(StorageEnvironment, OnChunkSerialize);
    onChunkWrite = MFUNC(StorageEnvironment, OnChunkWrite);
    onChunkMerge = MFUNC(StorageEnvironment, OnChunkMerge);
    onLogArchive = MFUNC(StorageEnvironment, OnLogArchive);
    onBackgroundTimer = MFUNC(StorageEnvironment, OnBackgroundTimer);
    backgroundTimer.SetDelay(STORAGE_BACKGROUND_TIMER_DELAY);
    backgroundTimer.SetCallable(onBackgroundTimer);
    
    nextChunkID = 1;
    nextLogSegmentID = 1;
    asyncLogSegmentID = 0;
    asyncWriteChunkID = 0;
    mergeShardID = 0;
    mergeContextID = 0;
    lastWriteTime = 0;
    haveUncommitedWrites = false;
    yieldThreads = false;
    numBulkCursors = 0;
    serializeChunk = NULL;
    writeChunk = NULL;
    mergeChunkIn1 = NULL;
    mergeChunkIn2 = NULL;
    mergeChunkOut = NULL;
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
    headLogSegment->Open(tmp, nextLogSegmentID);
    nextLogSegmentID++;

    EventLoop::Add(&backgroundTimer);

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
    asyncGetThread->Stop();
    delete asyncGetThread;
    
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

void StorageEnvironment::SetYieldThreads(bool yieldThreads_)
{
    yieldThreads = yieldThreads_;
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

void StorageEnvironment::AsyncGet(uint16_t contextID, uint64_t shardID, StorageAsyncGet* asyncGet)
{
    StorageShard*       shard;
    StorageChunk*       chunk;
    StorageKeyValue*    kv;
    Callable            onComplete = MFunc<StorageAsyncGet, &StorageAsyncGet::OnComplete>(asyncGet);
    Deferred            deferred(onComplete);

    asyncGet->completed = false;
    asyncGet->ret = false;
    shard = GetShard(contextID, shardID);
    if (shard == NULL)
        return;
        
    if (!shard->RangeContains(asyncGet->key))
        return;

    chunk = shard->GetMemoChunk();
    kv = chunk->Get(asyncGet->key);
    if (kv != NULL)
    {
        if (kv->GetType() == STORAGE_KEYVALUE_TYPE_DELETE)
        {
            asyncGet->completed = true;
            return;
        }
        else if (kv->GetType() == STORAGE_KEYVALUE_TYPE_SET)
        {
            asyncGet->ret = true;
            asyncGet->completed = true;
            asyncGet->value = kv->GetValue();
            return;
        }
        else
            ASSERT_FAIL();
    }

    if (shard->GetChunks().GetLength() == 0)
    {
        asyncGet->completed = true;
        return;
    }

    deferred.Unset();
    asyncGet->shard = shard;
    asyncGet->itChunk = shard->GetChunks().Last();
    asyncGet->lastLoadedPage = NULL;
    asyncGet->stage = StorageAsyncGet::START;
    asyncGet->threadPool = asyncGetThread;
    asyncGet->ExecuteAsyncGet();
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
    assert(memoChunk != NULL);
    if (!memoChunk->Set(key, value))
    {
        headLogSegment->Undo();
        return false;
    }
    memoChunk->RegisterLogCommand(headLogSegment->GetLogSegmentID(), logCommandID);

    haveUncommitedWrites = true;

    return true;
}

bool StorageEnvironment::Delete(uint16_t contextID, uint64_t shardID, ReadBuffer key)
{
    int32_t             logCommandID;
    StorageShard*       shard;
    StorageMemoChunk*   memoChunk;
    StorageChunk*       oldest;
    StorageFileChunk*   itFileChunk;
    StorageJob*         job;

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
        assert(memoChunk != NULL);
        if (!memoChunk->Delete(key))
        {
            headLogSegment->Undo();
            return false;
        }
        memoChunk->RegisterLogCommand(headLogSegment->GetLogSegmentID(), logCommandID);

        haveUncommitedWrites = true;
    }
    else if (shard->GetChunks().First())
    {
        oldest = *shard->GetChunks().First();
        if (oldest->GetChunkState() == StorageChunk::Written)
        {
            if (ReadBuffer::LessThan(oldest->GetLastKey(), key))
            {
                ((StorageFileChunk*) oldest)->RemovePagesFromCache();
                shard->GetChunks().Remove(oldest);
                FOREACH(itFileChunk, fileChunks)
                {
                    if (itFileChunk == (StorageFileChunk*) oldest)
                    {
                        fileChunks.Remove((StorageFileChunk*) oldest);
                        break;
                    }
                }
                WriteTOC();
                job = new StorageDeleteFileChunkJob((StorageFileChunk*) oldest);
                StartJob(writerThread, job);
            }
        }
    }
    
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
    
    numBulkCursors++;
    
    return bc;
}

uint64_t StorageEnvironment::GetSize(uint16_t contextID, uint64_t shardID)
{
    uint64_t        size;
    StorageShard*   shard;
    StorageChunk*   chunk;
    StorageChunk**  itChunk;

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

ReadBuffer StorageEnvironment::GetMidpoint(uint16_t contextID, uint64_t shardID)
{
    unsigned        numChunks, i;
    StorageShard*   shard;
    StorageChunk**  itChunk;

    shard = GetShard(contextID, shardID);
    if (!shard)
        return 0;
    
    numChunks = shard->GetChunks().GetLength();
    
    i = 0;
    FOREACH(itChunk, shard->GetChunks())
    {
        if (i == ((numChunks + 1) / 2))
            return (*itChunk)->GetMidpoint();

        i++;
    }
    
    return shard->GetMemoChunk()->GetMidpoint();
}

bool StorageEnvironment::IsSplittable(uint16_t contextID, uint64_t shardID)
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

    shard = new StorageShard;
    shard->SetContextID(contextID);
    shard->SetTableID(tableID);
    shard->SetShardID(shardID);
    shard->SetFirstKey(firstKey);
    shard->SetLastKey(lastKey);
    shard->SetUseBloomFilter(useBloomFilter);
    shard->SetIsLogStorage(isLogStorage);
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

    // delete memoChunk
    if (shard->GetMemoChunk() != NULL)
    {
        memoChunk = shard->GetMemoChunk();
        Log_Message("Deleting MemoChunk...");
        job = new StorageDeleteMemoChunkJob(memoChunk);
        StartJob(asyncThread, job);
        shard->memoChunk = NULL; // TODO: private hack
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
                    goto EndLoops;
                }
            }
        }
        
        EndLoops:
        if (!found)
        {
            // TOOD: delete chunk
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
                fileChunks.Remove((StorageFileChunk*) *chunk);
                if (writeChunk == *chunk || mergeChunkIn1 == *chunk || mergeChunkIn2 == *chunk)
                {
                    fileChunk->deleted = true;
                }
                else
                {
                    fileChunk->RemovePagesFromCache();
                    job = new StorageDeleteFileChunkJob(fileChunk);
                    StartJob(asyncThread, job);                    
                }
            }
            chunk = shard->GetChunks().Remove(chunk);
        }
        else
            chunk = shard->GetChunks().Next(chunk);
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
    bool asyncCommit;
    
    asyncCommit = commitThreadActive;
    
    commitThreadActive = false;
    haveUncommitedWrites = false;

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
    headLogSegment->Open(tmp, nextLogSegmentID);
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

    if (haveUncommitedWrites)
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
            serializeChunk = memoChunk;
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
    StorageFileChunk*       chunk1;
    StorageFileChunk*       chunk2;
    StorageJob*             job;    

#ifdef STORAGE_NOMERGE
    return;
#endif

    if (writerThreadActive)
        return;

    if (EventLoop::Now() - lastWriteTime < STORAGE_MERGE_AFTER_WRITE_DELAY)
        return;

    if (numBulkCursors > 0)
        return;

    FOREACH (itShard, shards)
    {
        if (itShard->IsLogStorage())
            continue;
            
        if (itShard->GetChunks().GetLength() >= 2)
        {
            chunk1 = (StorageFileChunk*) *(itShard->GetChunks().First());                               // first
            chunk2 = (StorageFileChunk*) *(itShard->GetChunks().Next(itShard->GetChunks().First()));    // second
            
            if (chunk1->GetChunkState() != StorageChunk::Written)
                continue;
            if (chunk2->GetChunkState() != StorageChunk::Written)
                continue;
            
            mergeChunkIn1 = chunk1;
            mergeChunkIn2 = chunk2;
            mergeChunkOut = new StorageFileChunk;
            mergeChunkOut->headerPage.SetChunkID(nextChunkID++);
            mergeChunkOut->headerPage.SetUseBloomFilter(itShard->UseBloomFilter());
            tmp.Write(chunkPath);
            tmp.Appendf("chunk.%020U", mergeChunkOut->GetChunkID());
            mergeChunkOut->SetFilename(ReadBuffer(tmp));            
            job = new StorageMergeChunkJob(
             this,
             ReadBuffer(chunk1->GetFilename()), ReadBuffer(chunk2->GetFilename()),
             mergeChunkOut, itShard->GetFirstKey(), itShard->GetLastKey(), &onChunkMerge);
            mergeShardID = itShard->GetShardID();
            mergeContextID = itShard->GetContextID();
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
    FOREACH (itShard, shards)
    {
        memoChunk = itShard->GetMemoChunk();
        if (memoChunk->GetMinLogSegmentID() > 0 && memoChunk->GetMinLogSegmentID() <= logSegmentID)
            archive = false;
        FOREACH (itChunk, itShard->GetChunks())
        {
            if ((*itChunk)->GetChunkState() <= StorageChunk::Unwritten)
            {
                assert((*itChunk)->GetMinLogSegmentID() > 0);
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
                    itShard->OnChunkSerialized(memoChunk, fileChunk);
                    Log_Message("Deleting MemoChunk...");
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
    StorageFileChunk*   chunk1;
    StorageFileChunk*   chunk2;
    StorageJob*         job;
    StorageChunk**      itChunk;
    StorageFileChunk*   itFileChunk;
    bool                delete1, delete2, deleteOut;

    writerThreadActive = false;

    assert(mergeChunkOut->written);
    
    if (numBulkCursors > 0)
    {
        job = new StorageDeleteFileChunkJob(mergeChunkOut);
        StartJob(writerThread, job);        
        mergeChunkIn1 = NULL;
        mergeChunkIn2 = NULL;
        mergeChunkOut = NULL;
        mergeContextID = 0;
        mergeShardID = 0;
        return;
    }

    itShard = GetShard(mergeContextID, mergeShardID);

    if (itShard == NULL)
    {
        // shard has been deleted
        delete1 = true;
        delete2 = true;
        deleteOut = true;
        goto Delete;
    }
    
    chunk1 = (StorageFileChunk*) *(itShard->GetChunks().First());                               // first
    chunk2 = (StorageFileChunk*) *(itShard->GetChunks().Next(itShard->GetChunks().First()));    // second

    itShard->GetChunks().Remove(itShard->GetChunks().First()); // remove both
    itShard->GetChunks().Remove(itShard->GetChunks().First()); // from this shard
    itShard->GetChunks().Add(mergeChunkOut);

    WriteTOC();

    delete1 = true;
    delete2 = true;
    deleteOut = false;
    FOREACH (itShard, shards)
    {
        FOREACH (itChunk, itShard->GetChunks())
        {
            if ((*itChunk) == chunk1)
                delete1 = false;
            if ((*itChunk) == chunk2)
                delete2 = false;
        }
    }

Delete:
    if (delete1)
    {
        chunk1->RemovePagesFromCache();
        FOREACH(itFileChunk, fileChunks)
        {
            if (itFileChunk == chunk1)
            {
                fileChunks.Remove(chunk1);
                break;
            }
        }
        job = new StorageDeleteFileChunkJob(chunk1);
        StartJob(writerThread, job);
    }
    if (delete2)
    {
        chunk2->RemovePagesFromCache();
        FOREACH(itFileChunk, fileChunks)
        {
            if (itFileChunk == chunk2)
            {
                fileChunks.Remove(chunk2);
                break;
            }
        }
        job = new StorageDeleteFileChunkJob(chunk2);
        StartJob(writerThread, job);
    }

    if (deleteOut)
    {
        job = new StorageDeleteFileChunkJob(mergeChunkOut);
        StartJob(writerThread, job);
    }
    else
    {
        fileChunks.Append(mergeChunkOut);
    }

    mergeChunkIn1 = NULL;
    mergeChunkIn2 = NULL;
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
    
    assert(asyncLogSegmentID == it->GetLogSegmentID());
    
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
