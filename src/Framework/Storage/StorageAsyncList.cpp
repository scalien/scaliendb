#include "StorageAsyncList.h"
#include "System/Threading/ThreadPool.h"
#include "System/IO/IOProcessor.h"
#include "StorageMemoChunkLister.h"
#include "StorageFileChunkLister.h"
#include "StorageUnwrittenChunkLister.h"
#include "StorageShard.h"
#include "StorageEnvironment.h"

#define MAX_RESULT_SIZE     64*KiB

StorageAsyncListResult::StorageAsyncListResult(StorageAsyncList* asyncList_) :
 dataPage(NULL, 0)
{
    asyncList = asyncList_;
    final = false;
}

// this is called from main thread
void StorageAsyncListResult::OnComplete()
{
    asyncList->lastResult = this;
    Call(onComplete);
    if (final)
    {
        asyncList->env->DecreaseNumCursors();
        asyncList->Clear();
    }
    delete this;
}

void StorageAsyncListResult::Append(StorageFileKeyValue* kv)
{
    dataPage.Append(kv);
}

uint32_t StorageAsyncListResult::GetSize()
{
    return dataPage.GetPageBufferSize();
}

StorageAsyncList::StorageAsyncList()
{
    count = 0;
    offset = 0;
    Init();
}

void StorageAsyncList::Init()
{    
    ret = false;
    completed = false;
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

    Log_Debug("StorageAsyncList cleared");
    
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
    StorageChunk**                  itChunk;
    StorageFileChunk*               fileChunk;
    StorageFileChunkLister*         fileLister;
    StorageMemoChunkLister*         memoLister;
    StorageUnwrittenChunkLister*    unwrittenLister;
    Buffer*                         filename;
    StorageChunk::ChunkState        chunkState;
    ReadBuffer                      firstKey;
    
    firstKey.Wrap(shardFirstKey);
    
    if (stage == START)
    {
        shardLastKey.Write(shard->GetLastKey());
        numChunks = shard->GetChunks().GetLength() + 1;

        listers = new StorageChunkLister*[numChunks];
        iterators = new StorageFileKeyValue*[numChunks];
        numListers = 0;

        FOREACH (itChunk, shard->GetChunks())
        {
            chunkState = (*itChunk)->GetChunkState();
            
            if (chunkState == StorageChunk::Serialized)
            {
                memoLister = new StorageMemoChunkLister;
                memoLister->Init((StorageMemoChunk*) *itChunk, firstKey, count, offset);
                listers[numListers] = memoLister;
                numListers++;
            }
            else if (chunkState == StorageChunk::Unwritten)
            {
                unwrittenLister = new StorageUnwrittenChunkLister;
                unwrittenLister->Init((StorageFileChunk*) *itChunk, firstKey, count, offset);
                listers[numListers] = unwrittenLister;
                numListers++;
            }
            else if (chunkState == StorageChunk::Written)
            {
                fileChunk = (StorageFileChunk*) *itChunk;
                filename = &fileChunk->GetFilename();
                fileLister = new StorageFileChunkLister;
                fileLister->Init(fileChunk->GetFilename(), (type == KEY || type == COUNT));
                listers[numListers] = fileLister;
                numListers++;
            }
        }
        
        stage = MEMO_CHUNK;
    }

    if (stage == MEMO_CHUNK)
    {
        LoadMemoChunk();
        stage = FILE_CHUNK;
    }
    
    if (stage == FILE_CHUNK)
    {
        threadPool->Execute(MFUNC(StorageAsyncList, AsyncLoadChunks));
    }
}

void StorageAsyncList::LoadMemoChunk()
{
    StorageMemoChunkLister* memoLister;
    ReadBuffer              firstKey;
    
    firstKey.Wrap(shardFirstKey);
    
    memoLister = new StorageMemoChunkLister;
    memoLister->Init(shard->GetMemoChunk(), firstKey, count, offset);

    // memochunk is always on the last position, because it is the most current
    listers[numListers] = memoLister;

    numListers++;
}

void StorageAsyncList::AsyncLoadChunks()
{
    unsigned    i;
    ReadBuffer  firstKey;
    
    firstKey.Wrap(shardFirstKey);
    for (i = 0; i < numListers; i++)
    {
        listers[i]->Load();
        iterators[i] = listers[i]->First(firstKey);
    }
    
    stage = MERGE;
    AsyncMergeResult();
}

void StorageAsyncList::AsyncMergeResult()
{
    StorageFileKeyValue*    it;
    StorageAsyncListResult* result;
    
    result = new StorageAsyncListResult(this);
    
    while(!IsDone())
    {
        // TODO: Yield
        if (count != 0 && num >= count)
            break;
        
        it = Next();
        if (it == NULL)
            break;
        
        if (endKey.GetLength() != 0 && STORAGE_KEY_GREATER_THAN(it->GetKey(), endKey))
            break;
        
        if (offset > 0)
        {
            offset--;
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
    unsigned    i;
    unsigned    numActive;
    
    if (env->IsShuttingDown())
        return true;

    numActive = 0;
    for (i = 0; i < numListers; i++)
    {
        if (iterators[i] != NULL)
            numActive++;
    }
    if (numActive == 0)
        return true;

    return false;
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
            if (STORAGE_KEY_LESS_THAN(iterators[i]->GetKey(), key) &&
                iterators[i]->GetType() != STORAGE_KEYVALUE_TYPE_DELETE)
            {
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