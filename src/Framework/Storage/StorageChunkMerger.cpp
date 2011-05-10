#include "StorageChunkMerger.h"
#include "StorageKeyValue.h"
#include "StorageEnvironment.h"
#include "StorageConfig.h"
#include "PointerGuard.h"
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
    Buffer**    itFilename;
    Deferred    onFinish(MFUNC(StorageChunkMerger, OnMergeFinished));
    uint64_t    preloadThreshold;
    
    env = env_;
    mergeChunk = mergeChunk_;
    
    minLogSegmentID = 0;
    maxLogSegmentID = 0;
    maxLogCommandID = 0;
    
    // open readers
    numReaders = filenames.GetLength();
    readers = new StorageChunkReader[numReaders];
    i = 0;
    numKeys = 0;
    preloadThreshold = env->GetConfig().mergeBufferSize / numReaders;
    FOREACH (itFilename, filenames)
    {
        readers[i].Open(ReadBuffer(**itFilename), preloadThreshold);
        numKeys += readers[i].GetNumKeys();
        
        // set up segment and command IDs
        if (readers[i].GetMinLogSegmentID() < minLogSegmentID)
            minLogSegmentID = readers[i].GetMinLogSegmentID();
        
        if (readers[i].GetMaxLogSegmentID() >= maxLogSegmentID)
        {
            maxLogSegmentID = readers[i].GetMaxLogSegmentID();
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
    if (fd.Open(mergeChunk->GetFilename().GetBuffer(), FS_CREATE | FS_READWRITE) == INVALID_FD)
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

//    FS_Sync(fd.GetFD());
    
    FS_FileSeek(fd.GetFD(), 0, FS_SEEK_SET);
    offset = 0;

    if (!WriteHeaderPage())
        return false;

    FS_Sync(fd.GetFD());

    fd.Close();
    
    mergeChunk->written = true;

    // Don't unload index and bloom page
    onFinish.Unset();

    delete[] readers;
    readers = NULL;
    delete[] iterators;
    iterators = NULL;

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

StorageFileKeyValue* StorageChunkMerger::MergeKeyValue(StorageFileKeyValue* , StorageFileKeyValue* it2)
{
    if (it2->GetType() == STORAGE_KEYVALUE_TYPE_SET)
        return it2;
    else
        return NULL;
}

bool StorageChunkMerger::WriteBuffer()
{
    ssize_t     writeSize;
    uint64_t    syncGranularity;

    writeSize = writeBuffer.GetLength();
    if (FS_FileWrite(fd.GetFD(), writeBuffer.GetBuffer(), writeSize) != writeSize)
        return false;
    
    offset += writeSize;

    syncGranularity = env->GetConfig().syncGranularity;
    if (syncGranularity != 0 && offset - lastSyncOffset > syncGranularity)
    {
        FS_Sync(fd.GetFD());
        lastSyncOffset = offset;
    }

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
    
//    mergeChunk->headerPage.SetChunkID(memoChunk->GetChunkID());
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
    unsigned                nit;
    StorageFileKeyValue*    it;
    StorageDataPage*        dataPage;
    StorageDataPageGuard    dataPageGuard;
    ReadBuffer              key;

    // although numKeys is counted in Merge(), but it is only an approximation
    numKeys = 0;

    nit = 0;
    index = 0;
    dataPage = new StorageDataPage(mergeChunk, index);
    dataPage->SetOffset(offset);
    dataPageGuard.Set(dataPage);

    while(!IsDone())
    {
        if (env->shuttingDown || mergeChunk->deleted || !env->mergeEnabled)
        {
            Log_Debug("Aborting merge");
            return false;
        }
        
        // TODO: HACK
        while (env->yieldThreads || env->asyncGetThread->GetNumPending() > 0)
        {
            Log_Trace("Yielding...");
            MSleep(1);
        }

        it = Next(lastKey);
        if (it == NULL)
            break;
    
//        key = it->GetKey();
//        Log_Debug("Merge: %R", &key);
        
        if (this->firstKey.GetLength() == 0)
            this->firstKey.Write(it->GetKey());
        this->lastKey.Write(it->GetKey());
        
        if (mergeChunk->UseBloomFilter())
            mergeChunk->bloomPage->Add(it->GetKey());

        numKeys++;
        if (dataPage->GetNumKeys() == 0)
        {
            dataPage->Append(it);
            mergeChunk->indexPage->Append(it->GetKey(), index, offset);
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
                
                writeBuffer.Clear();
                dataPage->Write(writeBuffer);
                ASSERT(writeBuffer.GetLength() == dataPage->GetSize());
                if (!WriteBuffer())
                    return false;
                
                mergeChunk->AppendDataPage(NULL);
                index++;
                dataPage = new StorageDataPage(mergeChunk, index);
                dataPageGuard.Set(dataPage);
                dataPage->SetOffset(offset);
                dataPage->Append(it);
                mergeChunk->indexPage->Append(it->GetKey(), index, offset);
            }
        }
    }

    // write last datapage
    if (dataPage->GetNumKeys() > 0)
    {
        dataPage->Finalize();

        writeBuffer.Clear();
        dataPage->Write(writeBuffer);
        ASSERT(writeBuffer.GetLength() == dataPage->GetSize());
        if (!WriteBuffer())
            return false;

        mergeChunk->AppendDataPage(NULL);
        index++;

        // dataPage is deleted automatically
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

StorageFileKeyValue* StorageChunkMerger::Next(ReadBuffer& lastKey)
{
    unsigned                i;
    unsigned                smallest;
    StorageFileKeyValue*    it;
    ReadBuffer              key;
    
Restart:
    key.Reset();
    it = NULL;
    smallest = 0;

    // readers are sorted by relevance, first is the oldest, last is the latest
    for (i = 0; i < numReaders; i++)
    {
        if (iterators[i] != NULL) 
        {
            // check that keys are still in the merged interval
            if (lastKey.GetLength() > 0 && 
             ReadBuffer::Cmp(iterators[i]->GetKey(), lastKey) >= 0)
            {
                iterators[i] = NULL;
                continue;
            }
            
            // in case of key equality, the reader with the highest chunkID wins
            if (it != NULL && ReadBuffer::Cmp(iterators[i]->GetKey(), key) == 0)
            {
                // in case of DELETE, restart scanning from the first reader
                if (iterators[i]->GetType() == STORAGE_KEYVALUE_TYPE_DELETE)
                {
                    iterators[smallest] = readers[smallest].Next(iterators[smallest]);
                    iterators[i] = readers[i].Next(iterators[i]);
                    goto Restart;
                }
            
                iterators[smallest] = readers[smallest].Next(iterators[smallest]);
                it = iterators[i];                
                smallest = i;
                continue;
            }
            
            // find the next smallest key
            if (STORAGE_KEY_LESS_THAN(iterators[i]->GetKey(), key) &&
             iterators[i]->GetType() != STORAGE_KEYVALUE_TYPE_DELETE)
            {
                it = iterators[i];
                key = it->GetKey();
                smallest =  i;
            }
        }
    }

    // make progress in the reader that contained the smallest key
    if (it != NULL)
        iterators[smallest] = readers[smallest].Next(iterators[smallest]);
    
    return it;
}
