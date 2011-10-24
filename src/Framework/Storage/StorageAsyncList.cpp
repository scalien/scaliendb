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

void StorageAsyncListResult::Append(StorageFileKeyValue* kv, bool countOnly)
{
    numKeys++;
    if (!countOnly)
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
    num = 0;
    stage = START;
    threadPool = NULL;
    iterators = NULL;
    listers = NULL;
    numListers = 0;
    lastResult = NULL;
    env = NULL;
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
    StorageFileChunk*               fileChunk;
    StorageFileChunkLister*         fileLister;
    StorageMemoChunkLister*         memoLister;
    StorageUnwrittenChunkLister*    unwrittenLister;
    Buffer*                         filename;
    StorageChunk::ChunkState        chunkState;
    ReadBuffer                      firstKey;
    ReadBuffer                      endKey;
    bool                            keysOnly;
    
    Log_Debug("StorageAsyncList START");
    firstKey.Wrap(shardFirstKey);
    endKey.Wrap(shardLastKey);
    keysOnly = (type == KEY || type == COUNT);

    if (!forwardDirection && prefix.GetLength() > 0 && !firstKey.BeginsWith(prefix) && count > 0)
        count++;

    if (stage == START)
    {
        shardLastKey.Write(shard->GetLastKey());
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
                memoLister->Init((StorageMemoChunk*) *itChunk, firstKey, endKey, prefix, count, 
                 keysOnly, forwardDirection);
                listers[numListers] = memoLister;
                numListers++;
            }
            else if (chunkState == StorageChunk::Unwritten)
            {
                unwrittenLister = new StorageUnwrittenChunkLister;
                unwrittenLister->Init(*((StorageFileChunk*) *itChunk), firstKey, count, forwardDirection);
                listers[numListers] = unwrittenLister;
                numListers++;
            }
            else if (chunkState == StorageChunk::Written)
            {
                fileChunk = (StorageFileChunk*) *itChunk;
                filename = &fileChunk->GetFilename();
                fileLister = new StorageFileChunkLister;
                fileLister->Init(fileChunk->GetFilename(), firstKey, endKey, prefix, count, 
                 keysOnly, preloadBufferSize, forwardDirection);
                listers[numListers] = fileLister;
                numListers++;
            }
        }
        
        stage = MEMO_CHUNK;
    }

    Log_Debug("StorageAsyncList MEMO_CHUNK");

    if (stage == MEMO_CHUNK)
    {
        LoadMemoChunk(keysOnly);
        stage = FILE_CHUNK;
    }
    
    Log_Debug("StorageAsyncList FILE_CHUNK");
    
    if (stage == FILE_CHUNK)
    {
        threadPool->Execute(MFUNC(StorageAsyncList, AsyncLoadChunks));
    }
}

void StorageAsyncList::LoadMemoChunk(bool keysOnly)
{
    StorageMemoChunkLister* memoLister;
    ReadBuffer              firstKey;
    ReadBuffer              endKey;
    
    firstKey.Wrap(shardFirstKey);
    endKey.Wrap(shardLastKey);
    
    memoLister = new StorageMemoChunkLister;
    memoLister->Init(shard->GetMemoChunk(), firstKey, endKey, prefix, count, keysOnly, forwardDirection);

    // memochunk is always on the last position, because it is the most current
    listers[numListers] = memoLister;

    numListers++;
}

void StorageAsyncList::AsyncLoadChunks()
{
    unsigned    i;
    ReadBuffer  firstKey;
    ReadBuffer  endKey;
    
    firstKey.Wrap(shardFirstKey);
    endKey.Wrap(shardLastKey);

    for (i = 0; i < numListers; i++)
    {
        Log_Debug("AsyncLoadChunks: Loading %d", i);
        listers[i]->Load();
        Log_Debug("AsyncLoadChunks: Setting iterator to firstKey %R", &firstKey);
        iterators[i] = listers[i]->First(firstKey);
    }
    
    stage = MERGE;
    AsyncMergeResult();
}

void StorageAsyncList::AsyncMergeResult()
{
    StorageFileKeyValue*    it;
    StorageAsyncListResult* result;

    Log_Debug("starting AsyncMergeResult");

    result = new StorageAsyncListResult(this);

    if (!forwardDirection && prefix.GetLength() > 0 && !firstKey.BeginsWith(prefix) && count > 0)
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
            if (endKey.GetLength() != 0 && STORAGE_KEY_GREATER_THAN(it->GetKey(), endKey))
                break;
        }
        else
        {
            if (endKey.GetLength() != 0 && STORAGE_KEY_GREATER_THAN(endKey, it->GetKey()))
                break;
        }

        if (prefix.GetLength() != 0 && !it->GetKey().BeginsWith(prefix))
        {
            if (num == 0)
                continue;
            break;
        }
                
        result->Append(it, type == COUNT);
        num++;
                
        if (result->GetSize() > MAX_RESULT_SIZE)
        {
            OnResult(result);
            result = new StorageAsyncListResult(this);
        }
    }
    
    Log_Debug("finished AsyncMergeResult");
    
    result->final = true;
    OnResult(result);
}

void StorageAsyncList::OnResult(StorageAsyncListResult* result)
{
    Callable    callable;
    
    result->dataPage.Finalize();
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

StorageFileKeyValue* StorageAsyncList::Next()
{
    unsigned                i;
    unsigned                smallest;
    StorageFileKeyValue*    it;
    ReadBuffer              key;
    
Restart:
    key.Reset();
    it = NULL;
    smallest = 0;
    
    // TODO: special case when there is only one chunk

    // listers are sorted by relevance, first is the oldest, last is the latest
    for (i = 0; i < numListers; i++)
    {
        if (iterators[i] != NULL) 
        {
            // check that keys are still in the merged interval
            if (shardLastKey.GetLength() > 0 && 
             ReadBuffer::Cmp(iterators[i]->GetKey(), shardLastKey) >= 0)
            {
                iterators[i] = NULL;
                continue;
            }
            
            // in case of key equality, the lister with the highest chunkID wins
            if (it != NULL && ReadBuffer::Cmp(iterators[i]->GetKey(), key) == 0)
            {
                // in case of DELETE, restart scanning from the first lister
                if (iterators[i]->GetType() == STORAGE_KEYVALUE_TYPE_DELETE)
                {
                    iterators[smallest] = listers[smallest]->Next(iterators[smallest]);
                    iterators[i] = listers[i]->Next(iterators[i]);
                    goto Restart;
                }
                
                iterators[smallest] = listers[smallest]->Next(iterators[smallest]);
                it = iterators[i];                
                smallest = i;
                continue;
            }
            
            // find the next smallest key
            if ((forwardDirection && STORAGE_KEY_LESS_THAN(iterators[i]->GetKey(), key)) ||
             (!forwardDirection && STORAGE_KEY_LESS_THAN(key, iterators[i]->GetKey())))
            {
                if (iterators[i]->GetType() == STORAGE_KEYVALUE_TYPE_DELETE)
                {
                    iterators[i] = listers[i]->Next(iterators[i]);
                    goto Restart;
                }
                
                it = iterators[i];
                key = it->GetKey();
                smallest =  i;
            }
        }
    }

    // make progress in the lister that contained the smallest key
    if (it != NULL)
        iterators[smallest] = listers[smallest]->Next(iterators[smallest]);
    
    return it;
}
