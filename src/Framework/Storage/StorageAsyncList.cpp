#include "StorageAsyncList.h"
#include "System/Threading/ThreadPool.h"
#include "System/IO/IOProcessor.h"
#include "StorageMemoChunkLister.h"
#include "StorageFileChunkLister.h"
#include "StorageUnwrittenChunkLister.h"
#include "StorageShard.h"
#include "StorageEnvironment.h"

#define MAX_RESULT_SIZE     (1024*KiB)

StorageAsyncListResult::StorageAsyncListResult(StorageAsyncList* asyncList_) :
 dataPage(NULL, 0)
{
    asyncList = asyncList_;
    numKeys = 0;
    final = false;
}

// this is called from main thread
void StorageAsyncListResult::OnComplete()
{
    asyncList->lastResult = this;
    Call(onComplete);
    asyncList->lastResult = NULL;
    if (final)
    {
        asyncList->env->DecreaseNumCursors();
        asyncList->Clear();
    }
    delete this;
}

void StorageAsyncListResult::Append(StorageFileKeyValue* kv)
{
    numKeys++;
    dataPage.Append(kv);
}

uint32_t StorageAsyncListResult::GetSize()
{
    return dataPage.GetPageBufferSize();
}

StorageAsyncList::StorageAsyncList()
{
    count = 0;
    Init();
}

void StorageAsyncList::Init()
{    
    ret = false;
    completed = false;
    aborted = false;
    forwardDirection = true;
    startWithLastKey = false;
    num = 0;
    stage = START;
    threadPool = NULL;
    iterators = NULL;
    listers = NULL;
    numListers = 0;
    lastResult = NULL;
    env = NULL;
    requestID = 0;
}

void StorageAsyncList::Clear()
{
    unsigned    i;

    for (i = 0; i < numListers; i++)
        delete listers[i];
    delete[] listers;
    listers = NULL;
    delete[] iterators;
    iterators = NULL;
    numListers = 0;

    Init();
}

void StorageAsyncList::ExecuteAsyncList()
{
    unsigned                        numChunks;
    uint64_t                        preloadBufferSize;
    StorageChunk**                  itChunk;
    StorageFileChunkLister*         fileLister;
    StorageMemoChunkLister*         memoLister;
    StorageUnwrittenChunkLister*    unwrittenLister;
    StorageChunk::ChunkState        chunkState;
    bool                            keysOnly;
    
    Log_Debug("List[%U] StorageAsyncList START", requestID);
    keysOnly = (type == KEY || type == COUNT);

    if (!forwardDirection && prefix.GetLength() > 0 && !startKey.BeginsWith(prefix) && count > 0)
        count++;

    if (stage == START)
    {
        numChunks = shard->GetChunks().GetLength() + 1;

        listers = new StorageChunkLister*[numChunks];
        iterators = new StorageFileKeyValue*[numChunks];
        numListers = 0;
        preloadBufferSize = 0;  // preload only one page

        FOREACH (itChunk, shard->GetChunks())
        {
            chunkState = (*itChunk)->GetChunkState();
            
            if (chunkState == StorageChunk::Serialized)
            {
                memoLister = new StorageMemoChunkLister;
                memoLister->Init((StorageMemoChunk*) *itChunk, startKey, endKey, prefix, count, 
                 keysOnly, forwardDirection);
                listers[numListers] = memoLister;
                numListers++;
            }
            else if (chunkState == StorageChunk::Unwritten)
            {
                unwrittenLister = new StorageUnwrittenChunkLister;
                unwrittenLister->Init(*((StorageFileChunk*) *itChunk), startKey, count, forwardDirection);
                listers[numListers] = unwrittenLister;
                numListers++;
            }
            else if (chunkState == StorageChunk::Written)
            {
                fileLister = new StorageFileChunkLister;
                fileLister->Init((StorageFileChunk*) *itChunk, startKey, endKey, prefix, count, 
                 keysOnly, preloadBufferSize, forwardDirection);
                listers[numListers] = fileLister;
                numListers++;
            }
        }
        
        stage = MEMO_CHUNK;
    }

    Log_Debug("List[%U] StorageAsyncList MEMO_CHUNK", requestID);

    if (stage == MEMO_CHUNK)
    {
        LoadMemoChunk(keysOnly);
        stage = FILE_CHUNK;
    }
    
    Log_Debug("List[%U] StorageAsyncList FILE_CHUNK", requestID);
    
    if (stage == FILE_CHUNK)
    {
        threadPool->Execute(MFUNC(StorageAsyncList, AsyncLoadChunks));
    }
}

void StorageAsyncList::LoadMemoChunk(bool keysOnly)
{
    StorageMemoChunkLister* memoLister;
    
    memoLister = new StorageMemoChunkLister;
    memoLister->Init(shard->GetMemoChunk(), startKey, endKey, prefix, count, keysOnly, forwardDirection);

    // memochunk is always on the last position, because it is the most current
    listers[numListers] = memoLister;

    numListers++;
}

void StorageAsyncList::AsyncLoadChunks()
{
    unsigned    i;

    for (i = 0; i < numListers; i++)
    {
        Log_Debug("List[%U] Setting iterator to firstKey %R", requestID, &startKey);
        iterators[i] = listers[i]->First(startKey);

        if (iterators[i] != NULL && !IsKeyInShard(iterators[i]->GetKeyReference()))
            iterators[i] = NULL;
    }
    
    stage = MERGE;
    AsyncMergeResult();
}

void StorageAsyncList::AsyncMergeResult()
{
    StorageFileKeyValue*    it;
    StorageAsyncListResult* result;

    Log_Debug("List[%U] Starting AsyncMergeResult", requestID);

    result = new StorageAsyncListResult(this);

    if (!forwardDirection && prefix.GetLength() > 0 && !startKey.BeginsWith(prefix) && count > 0)
        count--;

    while(!IsDone())
    {
        // TODO: Yield
        if (count != 0 && num >= count)
            break;
        
        it = Next();
        if (it == NULL)
            break;
        
        if (forwardDirection)
        {
            if (endKey.GetLength() != 0 && STORAGE_KEY_GREATER_THAN(it->GetKeyReference(), endKey))
                break;
        }
        else
        {
            if (endKey.GetLength() != 0 && STORAGE_KEY_GREATER_THAN(endKey, it->GetKeyReference()))
                break;
        }

        if (prefix.GetLength() != 0 && !it->GetKey().BeginsWith(prefix))
        {
            if (num == 0)
                continue;
            break;
        }

        if (startWithLastKey && it->GetKey().Equals(shardLastKey.GetReadBuffer()))
        {
            startWithLastKey = false;
            continue;
        }

        result->Append(it);
        num++;
                
        if (result->GetSize() > MAX_RESULT_SIZE)
        {
            OnResult(result);
            result = new StorageAsyncListResult(this);
        }
    }
    
    Log_Debug("List[%U] Finished AsyncMergeResult", requestID);
    
    result->final = true;
    OnResult(result);
}

void StorageAsyncList::OnResult(StorageAsyncListResult* result)
{
    bool                    redoDataPage;
    Callable                callable;
    StorageFileKeyValue*    fkv;
    StorageAsyncListResult* newResult;
    
    result->dataPage.Finalize();

    // check that keys are still in the merged interval
    redoDataPage = false;
    if (result->dataPage.First() && !IsKeyInShard(result->dataPage.First()->GetKeyReference()))
        redoDataPage = true;
    if (result->dataPage.Last() && !IsKeyInShard(result->dataPage.Last()->GetKeyReference()))
        redoDataPage = true;

    if (redoDataPage)
    {
        newResult = new StorageAsyncListResult(this);
        FOREACH(fkv, result->dataPage)
        {
            if (IsKeyInShard(fkv->GetKeyReference()))
                newResult->Append(fkv);
        }
        newResult->dataPage.Finalize();
        newResult->final = result->final;
        delete result;
        result = newResult;
    }

    result->onComplete = onComplete;
    callable = MFunc<StorageAsyncListResult, &StorageAsyncListResult::OnComplete>(result);
    IOProcessor::Complete(&callable);
}

bool StorageAsyncList::IsDone()
{
    if (env->IsShuttingDown())
        return true;

    if (IsAborted())
        return true;

    return false;
}

bool StorageAsyncList::IsAborted()
{
    return aborted;
}

void StorageAsyncList::SetAborted(bool aborted_)
{
    aborted = aborted_;
}

#define ADVANCE_ITERATOR(i) iterators[i] = listers[i]->Next(iterators[i])

bool StorageAsyncList::IsKeyInShard(const ReadBuffer& key)
{
    if (forwardDirection)
    {
        if (shardLastKey.GetLength() > 0 && ReadBuffer::Cmp(key, shardLastKey) >= 0)
            return false;
    }
    else
    {
        if (shardFirstKey.GetLength() > 0 && ReadBuffer::Cmp(key, shardFirstKey) < 0)
            return false;
    }

    return true;
}

int StorageAsyncList::CompareSmallestKey(const ReadBuffer& key, const ReadBuffer& smallestKey)
{
    int     cmpres;

    if (forwardDirection)
    {
        if (smallestKey.GetLength() == 0)
            cmpres = -1;
        else
            cmpres = ReadBuffer::Cmp(key, smallestKey);        
    }
    else
    {
        if (key.GetLength() == 0)
            cmpres = -1;
        else
            cmpres = ReadBuffer::Cmp(smallestKey, key);
    }
    
    return cmpres;
}

StorageFileKeyValue* StorageAsyncList::GetSmallest()
{
    unsigned                i;
    unsigned                smallestIndex;
    ReadBuffer              smallestKey;
    StorageFileKeyValue*    smallestKv;
    int                     cmpres;
    ReadBuffer              tmp;

    smallestIndex = 0;  // making the compiler happy by initializing
    smallestKv = NULL;
    // readers are sorted by relevance, first is the oldest, last is the latest
        
    for (i = 0; i < numListers; i++)
    {
        if (iterators[i] == NULL)
            continue;

        cmpres = CompareSmallestKey(iterators[i]->GetKeyReference(), smallestKey);
        if (cmpres <= 0)
        {
            // advance the previously smallest if it is equal to the current
            // because it is less relevant
            if (smallestKv != NULL && cmpres == 0)
                ADVANCE_ITERATOR(smallestIndex);
            smallestKv = iterators[i];
            smallestIndex = i;
            smallestKey = smallestKv->GetKeyReference();
        }
    }

    // make progress in the reader that contained the smallest key
    if (smallestKv != NULL)
        ADVANCE_ITERATOR(smallestIndex);

    return smallestKv;
}

StorageFileKeyValue* StorageAsyncList::Next()
{
    StorageFileKeyValue*    kv;

    while (true)
    {
        kv = GetSmallest();
        if (kv == NULL)
            return NULL;    // reached the end of all chunkfiles

        if (kv->GetType() == STORAGE_KEYVALUE_TYPE_SET)
            return kv;
    }

    return NULL;
}
