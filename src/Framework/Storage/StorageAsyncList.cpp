#include "StorageAsyncList.h"
#include "System/ThreadPool.h"
#include "StorageMemoChunkLister.h"
#include "StorageFileChunkLister.h"
#include "StorageShard.h"
#include "StorageEnvironment.h"

#define MAX_RESULT_SIZE     64*KiB

StorageAsyncListResult::StorageAsyncListResult(StorageAsyncList* asyncList_) :
 dataPage(NULL, 0)
{
    asyncList = asyncList_;
    final = false;
}

void StorageAsyncListResult::OnComplete()
{
    dataPage.Finalize();
    asyncList->OnResult(this);
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
    keyValues = false;
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
    unsigned                numChunks;
    StorageChunk**          itChunk;
    StorageFileChunk*       fileChunk;
    StorageFileChunkLister* fileLister;
    Buffer*                 filename;
    
    if (stage == START)
    {
        numChunks = shard->GetChunks().GetLength() + 1;

        listers = new StorageChunkLister*[numChunks];
        iterators = new StorageFileKeyValue*[numChunks];
        numListers = 0;

        FOREACH (itChunk, shard->GetChunks())
        {
            if ((*itChunk)->GetChunkState() != StorageChunk::Written)
                continue;
            fileChunk = (StorageFileChunk*) *itChunk;
            filename = &fileChunk->GetFilename();
            fileLister = new StorageFileChunkLister;
            fileLister->SetFilename(fileChunk->GetFilename());
            listers[numListers] = fileLister;
            numListers++;
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
        threadPool->Execute(MFUNC(StorageAsyncList, AsyncLoadFileChunks));
    }
}

void StorageAsyncList::LoadMemoChunk()
{
    StorageMemoChunkLister* memoLister;
    
    memoLister = new StorageMemoChunkLister;
    memoLister->Load(shard->GetMemoChunk(), startKey, count, offset);

    // memochunk is always on the last position, because it is the most current
    listers[numListers] = memoLister;
    iterators[numListers] = memoLister->First(startKey);
    numListers++;
}

void StorageAsyncList::AsyncLoadFileChunks()
{
    unsigned                i;
    StorageFileChunkLister* fileLister;
    
    // TODO: HACK
    for (i = 0; i < numListers - 1; i++)
    {
        fileLister = (StorageFileChunkLister*) listers[i];
        fileLister->Load();
        iterators[i] = fileLister->First(startKey);
    }
    
    stage = MERGE;
    AsyncMergeResult();
}

void StorageAsyncList::AsyncMergeResult()
{
    StorageFileKeyValue*    it;
    StorageAsyncListResult* result;
    bool                    hasCount;
    
    hasCount = false;
    if (count > 0)
        hasCount = true;
    
    result = new StorageAsyncListResult(this);
    
    while(!IsDone())
    {
        // TODO: Yield
        
        it = Next();
        if (it == NULL)
            break;
        
        if (offset > 0)
        {
            offset--;
            continue;
        }
        
        result->Append(it);
        
        if (hasCount)
        {
            count--;
            if (count == 0)
                break;
        }
            
        if (result->GetSize() > MAX_RESULT_SIZE)
        {
            result->OnComplete();
            result = new StorageAsyncListResult(this);
        }
    }
    
    result->final = true;
    result->OnComplete();
    Clear();
}

void StorageAsyncList::OnResult(StorageAsyncListResult* result)
{
    lastResult = result;
    Call(onComplete);
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
    
    // listers are sorted by chunkID in increasing order
    // (chunks that are created later has higher ID)
    for (i = 0; i < numListers; i++)
    {
        if (iterators[i] != NULL) 
        {
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