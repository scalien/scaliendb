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
#include "StorageSerializeChunkJob.h"
#include "StorageWriteChunkJob.h"
#include "StorageMergeChunkJob.h"
#include "StorageDeleteMemoChunkJob.h"
#include "StorageDeleteFileChunkJob.h"
#include "StorageArchiveLogSegmentJob.h"

#define SERIALIZECHUNKJOB   ((StorageSerializeChunkJob*)(serializeChunkJobs.GetActiveJob()))
#define WRITECHUNKJOB       ((StorageWriteChunkJob*)(writeChunkJobs.GetActiveJob()))
#define MERGECHUNKJOB       ((StorageMergeChunkJob*)(mergeChunkJobs.GetActiveJob()))

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
    asyncThread = NULL;
    asyncGetThread = NULL;

    onBackgroundTimer = MFUNC(StorageEnvironment, OnBackgroundTimer);
    backgroundTimer.SetCallable(onBackgroundTimer);
    
    nextChunkID = 1;
    yieldThreads = false;
    shuttingDown = false;
    writingTOC = false;
    numCursors = 0;
    mergeEnabled = true;
}

bool StorageEnvironment::Open(Buffer& envPath_)
{
    char            lastChar;
    Buffer          tmp;
    StorageRecovery recovery;

    config.Init();

    commitJobs.Start();
    serializeChunkJobs.Start();
    writeChunkJobs.Start();
    mergeChunkJobs.Start();
    archiveLogJobs.Start();
    deleteChunkJobs.Start();

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

    backgroundTimer.SetDelay(1000 * configFile.GetIntValue("database.backgroundTimerDelay",
     STORAGE_DEFAULT_BACKGROUND_TIMER_DELAY));

    EventLoop::Add(&backgroundTimer);

    maxUnbackedLogSegments = configFile.GetIntValue("database.maxUnbackedLogSegments",
     STORAGE_DEFAULT_MAX_UNBACKED_LOG_SEGMENT);

    return true;
}

void StorageEnvironment::Close()
{
    StorageFileChunk*   fileChunk;
    HashNode<uint64_t, StorageLogSegment*>* itLogSegment;
    
    shuttingDown = true;

    commitJobs.Stop();
    serializeChunkJobs.Stop();
    writeChunkJobs.Stop();
    mergeChunkJobs.Stop();
    archiveLogJobs.Stop();
    deleteChunkJobs.Stop();
    
    asyncGetThread->Stop();
    asyncThread->Stop();
    delete asyncThread;
    delete asyncGetThread;
    
    shards.DeleteList();
    FOREACH(itLogSegment, logSegmentMap)
        delete itLogSegment->Value();
    logSegments.DeleteList();

    FOREACH (fileChunk, fileChunks)
        fileChunk->RemovePagesFromCache();
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
    
    FOREACH (itShard, shards)
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
    
    FOREACH (itShard, shards)
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
    Job*                job;
    StorageShard*       shard;
    StorageMemoChunk*   memoChunk;
    StorageLogSegment*  logSegment;
    
    ASSERT(key.GetLength() > 0);
    
    shard = GetShard(contextID, shardID);
    if (shard == NULL)
        return false;

    logSegment = GetLogSegment(shard->GetTrackID());
    if (!logSegment)
        ASSERT_FAIL();

    FOREACH(job, commitJobs)
        ASSERT(((StorageCommitJob*)job)->logSegment->GetTrackID() != shard->GetTrackID());

    logCommandID = logSegment->AppendSet(contextID, shardID, key, value);
    if (logCommandID < 0)
        ASSERT_FAIL();

    memoChunk = shard->GetMemoChunk();
    ASSERT(memoChunk != NULL);
    
    if (shard->IsLogStorage())
    {
        while (memoChunk->GetSize() > config.chunkSize)
            memoChunk->RemoveFirst();
    }
    
    if (!memoChunk->Set(key, value))
    {
        logSegment->Undo();
        return false;
    }
    memoChunk->RegisterLogCommand(logSegment->GetLogSegmentID(), logCommandID);

    return true;
}

bool StorageEnvironment::Delete(uint16_t contextID, uint64_t shardID, ReadBuffer key)
{
    int32_t             logCommandID;
    Job*                job;
    StorageShard*       shard;
    StorageMemoChunk*   memoChunk;
    StorageLogSegment*  logSegment;

    shard = GetShard(contextID, shardID);
    if (shard == NULL)
        return false;

    if (shard->IsLogStorage())
        ASSERT_FAIL();

    logSegment = GetLogSegment(shard->GetTrackID());
    if (!logSegment)
        ASSERT_FAIL();

    FOREACH(job, commitJobs)
        ASSERT(((StorageCommitJob*)job)->logSegment->GetTrackID() != shard->GetTrackID());

    logCommandID = logSegment->AppendDelete(contextID, shardID, key);
    if (logCommandID < 0)
        ASSERT_FAIL();

    memoChunk = shard->GetMemoChunk();
    ASSERT(memoChunk != NULL);
    if (!memoChunk->Delete(key))
    {
        logSegment->Undo();
        return false;
    }
    memoChunk->RegisterLogCommand(logSegment->GetLogSegmentID(), logCommandID);

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
        
        if (firstKey.GetLength() > 0 && !shard->RangeContains(firstKey))
        {
            size += chunk->GetPartialSize(shard->GetFirstKey(), shard->GetLastKey());
            continue;
        }

        if (lastKey.GetLength() > 0 && !shard->RangeContains(lastKey))
        {
            size += chunk->GetPartialSize(shard->GetFirstKey(), shard->GetLastKey());
            continue;
        }

        size += chunk->GetSize();
    }
    
    return size;
}

ReadBuffer StorageEnvironment::GetMidpoint(uint16_t contextID, uint64_t shardID)
{
    unsigned                i;
    StorageShard*           shard;
    StorageChunk**          itChunk;
    ReadBuffer              midpoint;
    SortedList<ReadBuffer>  midpoints;
    ReadBuffer*             itMidpoint;

    shard = GetShard(contextID, shardID);
    if (!shard)
        return ReadBuffer();

    midpoint = shard->GetMemoChunk()->GetMidpoint();
    if (midpoint.GetLength() > 0)
        midpoints.Add(midpoint);

    FOREACH (itChunk, shard->GetChunks())
    {
        midpoint = (*itChunk)->GetMidpoint();
        if (midpoint.GetLength() > 0)
            midpoints.Add(midpoint);
    }

    i = 0;
    FOREACH (itMidpoint, midpoints)
    {
        if (i >= (midpoints.GetLength() / 2))
            return *itMidpoint;
        i++;
    }
    
    return ReadBuffer();
}

bool StorageEnvironment::IsSplitable(uint16_t contextID, uint64_t shardID)
{
    StorageShard*   shard;
    StorageChunk**  itChunk;
    ReadBuffer      firstKey;
    ReadBuffer      lastKey;

    shard = GetShard(contextID, shardID);
    if (!shard)
        return false;

    return shard->IsSplitable();
    
    FOREACH (itChunk, shard->GetChunks())
    {
        firstKey = (*itChunk)->GetFirstKey();
        lastKey = (*itChunk)->GetLastKey();
        
        if (firstKey.GetLength() > 0 && !shard->RangeContains(firstKey))
            return false;

        if (lastKey.GetLength() > 0 && !shard->RangeContains(lastKey))
            return false;
    }
    
    return true;
}

bool StorageEnvironment::Commit(uint64_t trackID, Callable& onCommit)
{
    Job*                job;
    StorageLogSegment*  logSegment;

    Log_Debug("Commiting in track %U", trackID);
    
    logSegment = GetLogSegment(trackID);
    if (!logSegment)
        ASSERT_FAIL();

    FOREACH(job, commitJobs)
        ASSERT(((StorageCommitJob*)job)->logSegment->GetTrackID() != trackID);

    commitJobs.Execute(new StorageCommitJob(this, logSegment, onCommit));

    return true;
}

bool StorageEnvironment::Commit(uint64_t trackID)
{
    Job*                job;
    StorageLogSegment*  logSegment;

    Log_Debug("Commiting in track %U", trackID);

    logSegment = GetLogSegment(trackID);
    if (!logSegment)
        ASSERT_FAIL();

    FOREACH(job, commitJobs)
        ASSERT(((StorageCommitJob*)job)->logSegment->GetTrackID() != trackID);

    logSegment->Commit();
    OnCommit(NULL);

    return true;
}

bool StorageEnvironment::IsCommiting(uint64_t trackID)
{
    Job* job;

    FOREACH(job, commitJobs)
    {
        if (((StorageCommitJob*)job)->logSegment->GetTrackID() == trackID)
            return true;
    }
    
    return false;
}

bool StorageEnvironment::PushMemoChunk(uint16_t contextID, uint64_t shardID)
{
    StorageShard*       shard;
    StorageMemoChunk*   memoChunk;

    shard = GetShard(contextID, shardID);
    if (shard == NULL)
        return false;

    if (shard->IsLogStorage())
        return false; // never serialize log storage shards
    
    memoChunk = shard->GetMemoChunk();            
    shard->PushMemoChunk(new StorageMemoChunk(nextChunkID++, shard->UseBloomFilter()));

    serializeChunkJobs.Execute(new StorageSerializeChunkJob(this, memoChunk));
    
    return true;
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
    uint64_t            totalSize;
    
    buffer.Clear();
    totalSize = 0;
    
    FOREACH (shard, shards)
    {
        //if (shard->GetContextID() != contextID)
        //    continue;
        
        firstKey = shard->GetFirstKey();
        lastKey = shard->GetLastKey();
        midpoint = GetMidpoint(contextID, shard->GetShardID());
        isSplitable = IsSplitable(contextID, shard->GetShardID());

        buffer.Appendf("- shard %U (tableID = %U) \n", shard->GetShardID(), shard->GetTableID());
        buffer.Appendf("   track: %U\n", shard->GetTrackID());
        buffer.Appendf("   size: %s\n", HUMAN_BYTES(GetSize(shard->GetContextID(), shard->GetShardID())));
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
        buffer.Appendf("   logCommandID: %u\n", shard->GetLogCommandID());
        
        memoChunk = shard->GetMemoChunk();
        firstKey = memoChunk->GetFirstKey();
        lastKey = memoChunk->GetLastKey();
        midpoint = memoChunk->GetMidpoint();
        buffer.Appendf("    * memo chunk %U\n", memoChunk->GetChunkID());
        buffer.Appendf("       state: %d {0=Tree, 1=Serialized, 2=Unwritten, 3=Written}\n",
         memoChunk->GetChunkState());
        buffer.Appendf("       size: %s\n", HUMAN_BYTES(memoChunk->GetSize()));
        buffer.Appendf("       count: %u\n", memoChunk->keyValues.GetCount());
        MAKE_PRINTABLE(firstKey);
        buffer.Appendf("       firstKey: %B\n", &printable);
        MAKE_PRINTABLE(lastKey);
        buffer.Appendf("       lastKey: %B\n", &printable);
        MAKE_PRINTABLE(midpoint);
        buffer.Appendf("       midpoint: %B\n", &printable);
        buffer.Appendf("       minLogSegmentID: %U\n", memoChunk->GetMinLogSegmentID());
        buffer.Appendf("       maxLogSegmentID: %U\n", memoChunk->GetMaxLogSegmentID());
        buffer.Appendf("       maxLogCommandID: %u\n", memoChunk->GetMaxLogCommandID());
        totalSize += memoChunk->GetSize();

        FOREACH (itChunk, shard->GetChunks())
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
            buffer.Appendf("       maxLogCommandID: %u\n", (*itChunk)->GetMaxLogCommandID());
        }

        buffer.Appendf("\n");
    }

    buffer.Appendf("\nCache size: %s\n", HUMAN_BYTES(StoragePageCache::GetSize()));
    totalSize += StoragePageCache::GetSize();
    buffer.Appendf("Total memory size: %s", HUMAN_BYTES(totalSize));
}

uint64_t StorageEnvironment::GetShardMemoryUsage()
{
    uint64_t            totalSize;
    StorageShard*       shard;
    StorageChunk**      itChunk;

    totalSize = 0;
    
    FOREACH (shard, shards)
    {
        totalSize += shard->memoChunk->GetSize();

        FOREACH (itChunk, shard->GetChunks())
        {
            if ((*itChunk)->GetChunkState() != StorageChunk::Written)
                totalSize += (*itChunk)->GetSize();
        }
    }

    return totalSize;
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

bool StorageEnvironment::CreateShard(uint64_t trackID,
 uint16_t contextID, uint64_t shardID, uint64_t tableID,
 ReadBuffer firstKey, ReadBuffer lastKey, bool useBloomFilter, bool isLogStorage)
{
    bool                ret;
    uint64_t            logSegmentID, nextLogSegmentID;
    StorageShard*       shard;
    StorageLogSegment*  logSegment;

    // TODO: check for uncommited stuff    
    
    shard = GetShard(contextID, shardID);

    logSegment = GetLogSegment(trackID);
    if (!logSegment)
    {
        ret = logSegmentIDMap.Get(trackID, logSegmentID);
        if (!ret)
            logSegmentID = 1;
        nextLogSegmentID = logSegmentID + 1;
        logSegmentIDMap.Set(trackID, nextLogSegmentID);
        logSegment = new StorageLogSegment();
        ret = logSegment->Open(logPath, trackID, logSegmentID, config.syncGranularity);
        if (!ret)
            STOP_FAIL(1, "Cannot open database log file: %Blog.%020U.%020U", &logPath, trackID, logSegmentID);
        logSegmentMap.Set(trackID, logSegment);
    }

    if (shard != NULL)
        return false;       // already exists

    Log_Debug("Creating shard %u/%U", contextID, shardID);

    shard = new StorageShard;
    shard->SetTrackID(trackID);
    shard->SetContextID(contextID);
    shard->SetTableID(tableID);
    shard->SetShardID(shardID);
    shard->SetFirstKey(firstKey);
    shard->SetLastKey(lastKey);
    shard->SetUseBloomFilter(useBloomFilter);
    shard->SetLogStorage(isLogStorage);
    shard->SetLogSegmentID(logSegment->GetLogSegmentID());
    shard->SetLogCommandID(logSegment->GetLogCommandID());
    shard->PushMemoChunk(new StorageMemoChunk(nextChunkID++, useBloomFilter));

    shards.Append(shard);
    WriteTOC();
    return true;
}

void StorageEnvironment::DeleteShard(uint16_t contextID, uint64_t shardID)
{
    StorageShard*           shard;
    StorageChunk**          itChunk;
    StorageMemoChunk*       memoChunk;
    StorageFileChunk*       fileChunk;

    // TODO: check for uncommited stuff

    shard = GetShard(contextID, shardID);
    if (shard == NULL)
        return;        // does not exists

    Log_Debug("Deleting shard %u/%U", contextID, shardID);

    if (shard->GetMemoChunk() != NULL)
    {
        deleteChunkJobs.Enqueue(new StorageDeleteMemoChunkJob(shard->GetMemoChunk()));
        shard->memoChunk = NULL;
    }

    FOREACH_REMOVE(itChunk, shard->GetChunks())
    {
        if (GetNumShards(*itChunk) > 1)
            continue;

        if ((*itChunk)->GetChunkState() <= StorageChunk::Serialized)
        {
            memoChunk = (StorageMemoChunk*) *itChunk;
            if (serializeChunkJobs.IsActive() && SERIALIZECHUNKJOB->memoChunk == memoChunk)
                memoChunk->deleted = true;
            else
                deleteChunkJobs.Enqueue(new StorageDeleteMemoChunkJob(memoChunk));
        }
        else
        {
            fileChunk = (StorageFileChunk*) *itChunk;
            fileChunks.Remove(fileChunk);

            if ((mergeChunkJobs.IsActive() && MERGECHUNKJOB->inputChunks.Contains(fileChunk)) ||
                (writeChunkJobs.IsActive() && WRITECHUNKJOB->writeChunk == fileChunk))
            {
                fileChunk->deleted = true;
            }
            else
            {
                fileChunk->RemovePagesFromCache();
                deleteChunkJobs.Enqueue(new StorageDeleteFileChunkJob(fileChunk));
            }
        }
    }
    
    if (mergeChunkJobs.IsActive() && MERGECHUNKJOB->contextID == contextID && MERGECHUNKJOB->shardID == shardID)
        MERGECHUNKJOB->mergeChunk->deleted = true;

    shards.Remove(shard);
    delete shard;
    WriteTOC();
    deleteChunkJobs.Execute();    
    return;
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
    StorageLogSegment*      logSegment;

    shard = GetShard(contextID, newShardID);
    if (shard != NULL)
        return false;       // exists

    shard = GetShard(contextID, shardID);
    if (shard == NULL)
        return false;       // does not exist

    logSegment = GetLogSegment(shard->GetTrackID());
    if (!logSegment)
        ASSERT_FAIL();
    
    if (logSegment->HasUncommitted())
        Commit(shard->GetTrackID()); // TODO

    newShard = new StorageShard;
    newShard->SetTrackID(shard->GetTrackID());
    newShard->SetContextID(contextID);
    newShard->SetTableID(shard->GetTableID());
    newShard->SetShardID(newShardID);
    newShard->SetFirstKey(splitKey);
    newShard->SetLastKey(shard->GetLastKey());
    newShard->SetUseBloomFilter(shard->UseBloomFilter());
    newShard->SetLogSegmentID(logSegment->GetLogSegmentID());
    newShard->SetLogCommandID(logSegment->GetLogCommandID());

    FOREACH (itChunk, shard->GetChunks())
        newShard->PushChunk(*itChunk);

    memoChunk = shard->GetMemoChunk();

    newMemoChunk = new StorageMemoChunk(nextChunkID++, memoChunk->useBloomFilter);
    newMemoChunk->minLogSegmentID = memoChunk->minLogSegmentID;
    newMemoChunk->maxLogSegmentID = memoChunk->maxLogSegmentID;
    newMemoChunk->maxLogCommandID = memoChunk->maxLogCommandID;

    FOREACH (itKeyValue, memoChunk->keyValues)
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
    
    WriteTOC(); // TODO
    
    return true;
}

void StorageEnvironment::OnCommit(StorageCommitJob* job)
{
    if (job)
        Log_Debug("Commiting done in track %U", job->logSegment->GetTrackID());

    TryFinalizeLogSegments();
    TrySerializeChunks();
        
    if (job)
        Call(job->onCommit);

    delete job;
}

void StorageEnvironment::TryFinalizeLogSegments()
{
    bool                                    ret;
    uint64_t                                trackID;
    uint64_t                                logSegmentID, nextLogSegmentID;
    HashNode<uint64_t, StorageLogSegment*>* itLogSegment;
    StorageLogSegment*                      logSegment;
    
    FOREACH(itLogSegment, logSegmentMap)
    {
        trackID = itLogSegment->Key();
        logSegment = itLogSegment->Value();
        
        if (logSegment->GetOffset() < config.logSegmentSize)
            continue;

        logSegment->Close();
        logSegmentID = logSegment->GetLogSegmentID();

        logSegments.Append(logSegment);

        ret = logSegmentIDMap.Get(trackID, logSegmentID);
        if (!ret)
            logSegmentID = 1;
        nextLogSegmentID = logSegmentID + 1;
        logSegmentIDMap.Set(trackID, nextLogSegmentID);

        logSegment = new StorageLogSegment;
        logSegment->Open(logPath, itLogSegment->Key(), logSegmentID, config.syncGranularity);
        
        itLogSegment->Value() = logSegment;
    }
}

void StorageEnvironment::TrySerializeChunks()
{
    StorageShard*       itShard;
    StorageMemoChunk*   memoChunk;
    StorageLogSegment*  logSegment;

    if (serializeChunkJobs.IsActive())
        return;

    FOREACH (itShard, shards)
    {
        if (itShard->IsLogStorage())
            continue; // never serialize log storage shards
        
        memoChunk = itShard->GetMemoChunk();
                
        logSegment = GetLogSegment(itShard->GetTrackID());
        if (!logSegment)
            continue;

        if (
         memoChunk->GetSize() > config.chunkSize ||
         (
         logSegment->GetLogSegmentID() > maxUnbackedLogSegments &&
         memoChunk->GetMinLogSegmentID() > 0 &&
         memoChunk->GetMinLogSegmentID() < (logSegment->GetLogSegmentID() - maxUnbackedLogSegments)
         ))
        {
            if (memoChunk->GetSize() == 0)
                continue;

            itShard->PushMemoChunk(new StorageMemoChunk(nextChunkID++, itShard->UseBloomFilter()));
            serializeChunkJobs.Execute(new StorageSerializeChunkJob(this, memoChunk));
            return;
        }
    }
}

void StorageEnvironment::TryWriteChunks()
{
    StorageFileChunk*   itFileChunk;
    StorageLogSegment*  logSegment;
    
    if (writeChunkJobs.IsActive())
        return;

    FOREACH (itFileChunk, fileChunks)
    {
        logSegment = GetLogSegment(GetFirstShard(itFileChunk)->GetTrackID());
        if (!logSegment)
            continue;
        if ((itFileChunk->GetChunkState() == StorageChunk::Unwritten) &&
           ((itFileChunk->GetMaxLogSegmentID() < logSegment->GetLogSegmentID()) ||
            (itFileChunk->GetMaxLogSegmentID() == logSegment->GetLogSegmentID() &&
             itFileChunk->GetMaxLogCommandID() <= logSegment->GetCommitedLogCommandID())))
        {
            writeChunkJobs.Execute(new StorageWriteChunkJob(this, itFileChunk));
            return;
        }
    }
}

void StorageEnvironment::TryMergeChunks()
{
    StorageMergeChunkJob*   job;
    StorageShard*           itShard;
    StorageFileChunk*       mergeChunk;
    List<StorageFileChunk*> inputChunks;

    if (!mergeEnabled || mergeChunkJobs.IsActive() || numCursors > 0)
        return;

    FOREACH (itShard, shards)
    {
        inputChunks.Clear();
        itShard->GetMergeInputChunks(inputChunks);
        if (inputChunks.GetLength() == 0)
            continue;
        
        mergeChunk = new StorageFileChunk;
        mergeChunk->headerPage.SetChunkID(nextChunkID++);
        mergeChunk->headerPage.SetUseBloomFilter(itShard->UseBloomFilter());
        mergeChunk->SetFilename(chunkPath, mergeChunk->GetChunkID());

        job = new StorageMergeChunkJob(
         this, itShard->GetContextID(), itShard->GetShardID(),
         inputChunks, mergeChunk,
         itShard->GetFirstKey(), itShard->GetLastKey());
        mergeChunkJobs.Execute(job);
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
    
    if (archiveLogJobs.IsActive() || logSegments.GetLength() == 0)
        return;

    FOREACH(logSegment, logSegments)
    {
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
            if (itShard->GetTrackID() != logSegment->GetTrackID())
                continue;

            memoChunk = itShard->GetMemoChunk();
            if (memoChunk->GetSize() > 0 &&
             memoChunk->GetMinLogSegmentID() > 0 && memoChunk->GetMinLogSegmentID() <= logSegmentID)
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
            archiveLogJobs.Execute(new StorageArchiveLogSegmentJob(this, logSegment, archiveScript));
            return;
        }
    }
}

void StorageEnvironment::OnChunkSerialize(StorageSerializeChunkJob* job)
{
    Buffer              tmp;
    StorageFileChunk*   fileChunk;

    if (!job->memoChunk->deleted)
    {
        fileChunk = job->memoChunk->RemoveFileChunk();
        ASSERT(fileChunk);
        OnChunkSerialized(job->memoChunk, fileChunk);
        fileChunks.Append(fileChunk);        
    }

    deleteChunkJobs.Execute(new StorageDeleteMemoChunkJob(job->memoChunk));
    
    TrySerializeChunks();
    TryWriteChunks();
    delete job;
}

void StorageEnvironment::OnChunkWrite(StorageWriteChunkJob* job)
{
    if (!job->writeChunk->deleted)
    {
        job->writeChunk->written = true;    
        job->writeChunk->AddPagesToCache();
    }
    else
        deleteChunkJobs.Execute(new StorageDeleteFileChunkJob(job->writeChunk));

    WriteTOC();
    TryArchiveLogSegments();
    TryWriteChunks();
    TryMergeChunks();    
    delete job;
}

void StorageEnvironment::OnChunkMerge(StorageMergeChunkJob* job)
{
    StorageShard*       shard;
    StorageFileChunk**  itInputChunk;
    StorageFileChunk*   inputChunk;
    
    shard = GetShard(job->contextID, job->shardID);

    if (shard != NULL && job->mergeChunk->written)
    {
        // delete the input chunks from the shard
        FOREACH (itInputChunk, job->inputChunks)
            shard->GetChunks().Remove((StorageChunk*&)*itInputChunk);
        // add the new chunk to the shard
        if (!job->mergeChunk->IsEmpty())
            shard->GetChunks().Add(job->mergeChunk);
        WriteTOC(); // TODO: async
    }
    
    // enqueue the input chunks for deleting
    FOREACH (itInputChunk, job->inputChunks)
    {
        inputChunk = *itInputChunk;
        // unless the input chunk belongs other shards
        if (GetNumShards(inputChunk) > 0)
            continue;

        if (!(job->mergeChunk->written || inputChunk->deleted))
            continue;
        
        inputChunk->RemovePagesFromCache();
        if (!inputChunk->deleted)
            fileChunks.Remove(inputChunk);

        deleteChunkJobs.Execute(new StorageDeleteFileChunkJob(inputChunk));
    }

    if (shard != NULL && job->mergeChunk->written && !job->mergeChunk->IsEmpty())
    {
        fileChunks.Append(job->mergeChunk);
        job->mergeChunk->AddPagesToCache();
    }
    else
        deleteChunkJobs.Execute(new StorageDeleteFileChunkJob(job->mergeChunk));
    
    TryMergeChunks();
    delete job;
}

void StorageEnvironment::OnLogArchive(StorageArchiveLogSegmentJob* job)
{
    logSegments.Delete(job->logSegment);
    
    TryArchiveLogSegments();
    delete job;
}

void StorageEnvironment::OnBackgroundTimer()
{
    TrySerializeChunks();
    TryWriteChunks();
    TryMergeChunks();
    TryArchiveLogSegments();
    
    EventLoop::Add(&backgroundTimer);
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
    StorageShard*   itShard;
    StorageChunk*   pChunk;
    
    pChunk = (StorageChunk*) memoChunk;
    
    FOREACH (itShard, shards)
        if (itShard->GetChunks().Contains(pChunk))
            itShard->OnChunkSerialized(memoChunk, fileChunk);
}

unsigned StorageEnvironment::GetNumShards(StorageChunk* chunk)
{
    unsigned        count;
    StorageShard*   itShard;
    
    count = 0;
    FOREACH (itShard, shards)
        if (itShard->GetChunks().Contains(chunk))
            count++;
    
    return count;
}

StorageShard* StorageEnvironment::GetFirstShard(StorageChunk* chunk)
{
    StorageShard*   itShard;
    
    FOREACH (itShard, shards)
    {
        if (itShard->GetMemoChunk() == chunk)
            return itShard;
        if (itShard->GetChunks().Contains(chunk))
            return itShard;
    }
    
    return NULL;
}

StorageLogSegment* StorageEnvironment::GetLogSegment(uint64_t trackID)
{
    bool                ret;
    StorageLogSegment*  logSegment;
    
    ret = logSegmentMap.Get(trackID, logSegment);
    if (!ret)
        return NULL;
    return logSegment;
}

static size_t Hash(uint64_t h)
{
    return h;
}

static inline bool LessThan(ReadBuffer& a, ReadBuffer& b)
{
    return ReadBuffer::Cmp(a, b) < 0 ? true : false;
}

static inline bool operator==(const ReadBuffer& a, const ReadBuffer& b)
{
    return ReadBuffer::Cmp(a, b) == 0 ? true : false;
}
