#include "StorageChunkMerger.h"
#include "StorageKeyValue.h"
#include "StorageEnvironment.h"
#include "StorageConfig.h"
#include "System/PointerGuard.h"
#include "System/FileSystem.h"
#include "System/Events/EventLoop.h"
#include "System/Events/Deferred.h"

bool StorageChunkMerger::Merge(
 StorageEnvironment* env_,
 List<Buffer*>& filenames, StorageFileChunk* mergeChunk_,  
 ReadBuffer firstKey, ReadBuffer lastKey)
{
    unsigned    i;
    unsigned    numKeys;
    uint64_t    preloadThreshold;
    Buffer**    itFilename;
    Deferred    onFinish(MFUNC(StorageChunkMerger, OnMergeFinished));
    
    env = env_;
    mergeChunk = mergeChunk_;
    mergeChunk->writeError = true;

    minLogSegmentID = 0;
    maxLogSegmentID = 0;
    maxLogCommandID = 0;
    lastNumReads = 0;
    
    // open readers
    numReaders = filenames.GetLength();
    readers = new StorageChunkReader[numReaders];
    i = 0;
    numKeys = 0;
    preloadThreshold = env->GetConfig().GetMergeBufferSize() / numReaders;
    FOREACH (itFilename, filenames)
    {
        lastReadTime = EventLoop::Now();
        readers[i].Open(ReadBuffer(**itFilename), preloadThreshold);
        YieldDiskReads();

        numKeys += readers[i].GetNumKeys();
        
        // set up segment and command IDs
        if (minLogSegmentID == 0 || readers[i].GetMinLogSegmentID() < minLogSegmentID)
            minLogSegmentID = readers[i].GetMinLogSegmentID();
        
        if (readers[i].GetMaxLogSegmentID() > maxLogSegmentID)
        {
            maxLogSegmentID = readers[i].GetMaxLogSegmentID();
            maxLogCommandID = readers[i].GetMaxLogCommandID();
        }
        else if (readers[i].GetMaxLogSegmentID() == maxLogSegmentID)
        {
            if (readers[i].GetMaxLogCommandID() > maxLogCommandID)
                maxLogCommandID = readers[i].GetMaxLogCommandID();
        }
        
        i++;
    }
    
    // set up iterators
    iterators = new StorageFileKeyValue*[numReaders];
    for (i = 0; i < numReaders; i++)
        iterators[i] = readers[i].First(firstKey);
    
    // open writer
    if (fd.Open(mergeChunk->GetFilename().GetBuffer(), FS_CREATE | FS_WRITEONLY | FS_TRUNCATE) == INVALID_FD)
        return false;

    offset = 0;
    lastSyncOffset = 0;

    mergeChunk->indexPage = new StorageIndexPage(mergeChunk);
    if (mergeChunk->UseBloomFilter())
    {
        mergeChunk->bloomPage = new StorageBloomPage(mergeChunk);
        mergeChunk->bloomPage->SetNumKeys(numKeys);
    }

    if (!WriteEmptyHeaderPage())
        return false;

    if (!WriteDataPages(firstKey, lastKey))
        return false;

    if (!WriteIndexPage())
        return false;

    if (mergeChunk->UseBloomFilter())
    {
        if (!WriteBloomPage())
            return false;
    }

    mergeChunk->fileSize = offset;

    FS_FileSeek(fd.GetFD(), 0, FS_SEEK_SET);
    offset = 0;

    if (!WriteHeaderPage())
        return false;

    StorageEnvironment::Sync(fd.GetFD());

    fd.Close();
    
    mergeChunk->written = true;
    // unload data pages, because we don't want to add them
    // to the cache to avoid throwing out other pages
    mergeChunk->UnloadDataPages();

    // Don't unload index and bloom page
    onFinish.Unset();

    delete[] readers;
    readers = NULL;
    delete[] iterators;
    iterators = NULL;

    mergeChunk->writeError = false;

    return true;
}

void StorageChunkMerger::OnMergeFinished()
{
    delete[] readers;
    readers = NULL;
    delete[] iterators;
    iterators = NULL;

    UnloadChunk();
}

bool StorageChunkMerger::WriteBuffer()
{
    ssize_t     writeSize;
    uint64_t    syncGranularity;
    uint64_t    startTime;
    uint64_t    elapsed;

    startTime = EventLoop::Now();
    writeSize = writeBuffer.GetLength();
    if (FS_FileWrite(fd.GetFD(), writeBuffer.GetBuffer(), writeSize) != writeSize)
        return false;

    offset += writeSize;

    syncGranularity = env->GetConfig().GetSyncGranularity();
    if (syncGranularity != 0 && offset - lastSyncOffset > syncGranularity)
    {
        StorageEnvironment::Sync(fd.GetFD());
        lastSyncOffset = offset;
    }

    // let other disk activity to happen
    elapsed = EventLoop::Now() - startTime;
    if (elapsed > 50)
        MSleep(elapsed);

    return true;
}

void StorageChunkMerger::UnloadChunk()
{
    delete mergeChunk->indexPage;
    delete mergeChunk->bloomPage;
    mergeChunk->indexPage = NULL;
    mergeChunk->bloomPage = NULL;
}

bool StorageChunkMerger::WriteEmptyHeaderPage()
{
    uint32_t    pageSize;

    pageSize = mergeChunk->headerPage.GetSize();

    writeBuffer.Allocate(pageSize);
    writeBuffer.SetLength(pageSize);
    writeBuffer.Zero();

    if (!WriteBuffer())
        return false;

    return true;
}

bool StorageChunkMerger::WriteHeaderPage()
{
    mergeChunk->headerPage.SetOffset(0);
   
    mergeChunk->headerPage.SetMinLogSegmentID(minLogSegmentID);
    mergeChunk->headerPage.SetMaxLogSegmentID(maxLogSegmentID);
    mergeChunk->headerPage.SetMaxLogCommandID(maxLogCommandID);
    mergeChunk->headerPage.SetNumKeys(numKeys);

    mergeChunk->headerPage.SetIndexPageOffset(mergeChunk->indexPage->GetOffset());
    mergeChunk->headerPage.SetIndexPageSize(mergeChunk->indexPage->GetSize());
    if (mergeChunk->bloomPage)
    {
        mergeChunk->headerPage.SetBloomPageOffset(mergeChunk->bloomPage->GetOffset());
        mergeChunk->headerPage.SetBloomPageSize(mergeChunk->bloomPage->GetSize());
    }
    if (firstKey.GetLength() > 0)
    {
        mergeChunk->headerPage.SetFirstKey(ReadBuffer(firstKey));
        mergeChunk->headerPage.SetLastKey(ReadBuffer(lastKey));
        mergeChunk->headerPage.SetMidpoint(mergeChunk->indexPage->GetMidpoint());
    }
    mergeChunk->headerPage.SetMerged(true);

    writeBuffer.Clear();
    mergeChunk->headerPage.Write(writeBuffer);
    ASSERT(writeBuffer.GetLength() == mergeChunk->headerPage.GetSize());

    if (!WriteBuffer())
        return false;

    return true;
}

bool StorageChunkMerger::WriteDataPages(ReadBuffer /*firstKey*/, ReadBuffer lastKey)
{
    typedef PointerGuard<StorageDataPage>   StorageDataPageGuard;
    
    uint32_t                index;
    StorageFileKeyValue*    it;
    StorageDataPage*        dataPage;
    StorageDataPageGuard    dataPageGuard;
    ReadBuffer              key;
    uint64_t                pageOffset;

    // although numKeys is counted in Merge(), it is only an approximation
    numKeys = 0;

    index = 0;
    pageOffset = offset;
    dataPage = new StorageDataPage(mergeChunk, index);
    dataPage->SetOffset(pageOffset);
    dataPageGuard.Set(dataPage);
    writeBuffer.Clear();

    while(!IsDone())
    {
        if (env->shuttingDown || mergeChunk->deleted || !env->IsMergeEnabled())
        {
            Log_Debug("Aborting merge, chunkID: %U", mergeChunk->GetChunkID());
            mergeChunk->writeError = false;
            return false;
        }
        
        while (!env->IsMergeRunning())
        {
            Log_Trace("Yielding...");
            MSleep(10);

            if (env->shuttingDown || mergeChunk->deleted || !env->IsMergeEnabled())
            {
                Log_Debug("Aborting merge, chunkID: %U", mergeChunk->GetChunkID());
                mergeChunk->writeError = false;
                return false;
            }
        }

        it = Next(lastKey);
        if (it == NULL)
            break;
    
        YieldDiskReads();
        lastReadTime = EventLoop::Now();

        ASSERT(it->GetKey().GetLength() > 0);

        if (this->firstKey.GetLength() == 0)
            this->firstKey.Write(it->GetKey());
        this->lastKey.Write(it->GetKey());
        
        if (mergeChunk->UseBloomFilter())
            mergeChunk->bloomPage->Add(it->GetKey());

        numKeys++;
        if (dataPage->GetNumKeys() == 0)
        {
            dataPage->Append(it);
            mergeChunk->indexPage->Append(it->GetKey(), index, pageOffset);
        }
        else
        {
            if (dataPage->GetLength() + dataPage->GetIncrement(it) <= STORAGE_DEFAULT_DATA_PAGE_SIZE)
            {
                dataPage->Append(it);
            }
            else
            {
                dataPage->Finalize();
                pageOffset += dataPage->Serialize(writeBuffer);
                if (writeBuffer.GetLength() > env->GetConfig().GetWriteGranularity())
                {
                    if (!WriteBuffer())
                        return false;
                    writeBuffer.Clear();
                }

                mergeChunk->AppendDataPage(NULL);
                index++;
                dataPage = new StorageDataPage(mergeChunk, index);
                dataPageGuard.Set(dataPage);
                dataPage->SetOffset(pageOffset);
                dataPage->Append(it);
                mergeChunk->indexPage->Append(it->GetKey(), index, pageOffset);
            }
        }
    }

    // write last datapage
    if (dataPage->GetNumKeys() > 0)
    {
        dataPage->Finalize();
        dataPage->Serialize(writeBuffer);

        mergeChunk->AppendDataPage(NULL);
        index++;

        // dataPage is deleted automatically because it is contained in a guard
    }

    if (writeBuffer.GetLength() > 0)
    {
        if (!WriteBuffer())
            return false;
        writeBuffer.Clear();
    }
    
    mergeChunk->indexPage->Finalize();
    return true;
}

bool StorageChunkMerger::WriteIndexPage()
{
    mergeChunk->indexPage->SetOffset(offset);

    writeBuffer.Clear();
    mergeChunk->indexPage->Write(writeBuffer);
    ASSERT(writeBuffer.GetLength() == mergeChunk->indexPage->GetSize());

    if (!WriteBuffer())
        return false;

    return true;
}

bool StorageChunkMerger::WriteBloomPage()
{
    mergeChunk->bloomPage->SetOffset(offset);

    writeBuffer.Clear();
    mergeChunk->bloomPage->Write(writeBuffer);
    ASSERT(writeBuffer.GetLength() == mergeChunk->bloomPage->GetSize());

    if (!WriteBuffer())
        return false;

    return true;
}

bool StorageChunkMerger::IsDone()
{
    unsigned    i;
    unsigned    numActive;
    
    numActive = 0;
    for (i = 0; i < numReaders; i++)
    {
        if (iterators[i] != NULL)
            numActive++;
    }
    if (numActive == 0)
        return true;

    return false;
}

#define ADVANCE_ITERATOR(i) iterators[i] = readers[i].Next(iterators[i])

StorageFileKeyValue* StorageChunkMerger::GetSmallest()
{
    unsigned                i;
    unsigned                smallestIndex;
    ReadBuffer              smallestKey;
    StorageFileKeyValue*    smallestKv;
    int                     cmpres;

    smallestIndex = 0;  // making the compiler happy by initializing
    smallestKv = NULL;
    // readers are sorted by relevance, first is the oldest, last is the latest
    for (i = 0; i < numReaders; i++)
    {
        if (iterators[i] == NULL)
            continue;

        // do the comparison
        if (smallestKey.GetLength() == 0)
            cmpres = -1;
        else
            cmpres = ReadBuffer::Cmp(iterators[i]->GetKey(), smallestKey);

        if (cmpres <= 0)
        {
            // advance the previously smallest if it is equal to the current
            // because it is less relevant
            if (smallestKv != NULL && cmpres == 0)
                ADVANCE_ITERATOR(smallestIndex);
            smallestKv = iterators[i];
            smallestIndex = i;
            smallestKey = smallestKv->GetKey();
        }
    }

    // make progress in the reader that contained the smallest key
    if (smallestKv != NULL)
        ADVANCE_ITERATOR(smallestIndex);

    return smallestKv;
}

StorageFileKeyValue* StorageChunkMerger::Next(ReadBuffer& lastKey)
{
    StorageFileKeyValue*    kv;

    while (true)
    {
        kv = GetSmallest();
        if (kv == NULL)
            return NULL;    // reached the end of all chunkfiles

        // check that keys are still in the merged interval
        if (lastKey.GetLength() > 0 && ReadBuffer::Cmp(kv->GetKey(), lastKey) >= 0)
            return NULL;

        if (kv->GetType() == STORAGE_KEYVALUE_TYPE_SET)
            return kv;
    }

    return NULL;
}

void StorageChunkMerger::YieldDiskReads()
{
    uint64_t    waitTime;
    uint64_t    now;
    uint64_t    elapsed;
    uint64_t    fsWaitTime;
    uint64_t    numReads;
    uint64_t    mergeYieldFactor;
    unsigned    readsPerSec;
    unsigned    waitUnit;
    FS_Stat     stat;

    mergeYieldFactor = env->GetConfig().GetMergeYieldFactor();
    if (mergeYieldFactor == 0)
        return;

    waitTime = 0;
    now = EventLoop::Now();
    if (now == lastReadTime)
        return;

    // Yield the same amount of time the last operation took
    waitTime = now - lastReadTime;

    // Use Performance Counters to yield. Unfortunately they have one second resolution
    // which is very low for yielding and results in spiky read performance
    readsPerSec = GetDiskReadsPerSec();
    if (readsPerSec * 2 > waitTime)
        waitTime = readsPerSec * 2;

    // Yield based on the internal FileSystem module statistics. This gives the best
    // results, but its heuristics needs tweaking through the mergeYieldFactor
    // config variable.
    if (waitTime > 10)
    {
        // This is effectively a memcpy, so we only call it at most every 10 msec
        FS_GetStats(&stat);
        elapsed = now - lastReadTime;
        numReads = stat.numReads - lastNumReads;
        fsWaitTime = numReads / elapsed / mergeYieldFactor;
        if (fsWaitTime > waitTime)
            waitTime = fsWaitTime;
        
        lastNumReads = stat.numReads;
        lastReadTime = EventLoop::Now();

        if (waitTime > 1000)
            waitTime = 1000;
    }
    
    // Sleep in waitUnit units, so long waits can be interrupted.
    waitUnit = 20;
    while (waitTime >= waitUnit) 
    {
        if (env->shuttingDown || mergeChunk->deleted || !env->IsMergeEnabled())
            return;

        MSleep(waitUnit);
        waitTime -= waitUnit;
    }
}
