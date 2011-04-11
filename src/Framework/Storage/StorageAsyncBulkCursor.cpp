#include "StorageAsyncBulkCursor.h"
#include "StorageChunkReader.h"
#include "StorageEnvironment.h"
#include "StorageMemoChunkLister.h"
#include "StorageUnwrittenChunkLister.h"
#include "System/Events/Callable.h"
#include "System/IO/IOProcessor.h"

#define MAX_PRELOAD_THRESHOLD   1*MB

StorageAsyncBulkResult::StorageAsyncBulkResult(StorageAsyncBulkCursor* cursor_) :
 dataPage(NULL, 0)
{
    cursor = cursor_;
    last = false;
}

void StorageAsyncBulkResult::OnComplete()
{
    cursor->lastResult = this;
    Call(onComplete);
    cursor->lastResult = NULL;    
    delete this;
}

StorageAsyncBulkCursor::StorageAsyncBulkCursor()
{
    env = NULL;
    shard = NULL;
    lastResult = NULL;
    itChunk = NULL;
    threadPool = NULL;
}

void StorageAsyncBulkCursor::SetEnvironment(StorageEnvironment* env_)
{
    env = env_;
}

void StorageAsyncBulkCursor::SetShard(uint64_t contextID, uint64_t shardID)
{
    shard = env->GetShard(contextID, shardID);
    ASSERT(shard);
}

void StorageAsyncBulkCursor::SetThreadPool(ThreadPool* threadPool_)
{
    threadPool = threadPool_;
}

void StorageAsyncBulkCursor::SetOnComplete(Callable onComplete_)
{
    onComplete = onComplete_;
}

StorageAsyncBulkResult* StorageAsyncBulkCursor::GetLastResult()
{
    return lastResult;
}

void StorageAsyncBulkCursor::OnNextChunk()
{
    ReadBuffer                  startKey;
    StorageFileChunk*           fileChunk;
    StorageMemoChunk*           memoChunk;
    StorageAsyncBulkResult*     result;
    StorageMemoChunkLister      memoLister;
    StorageUnwrittenChunkLister unwrittenLister;
    
    if (itChunk == NULL)
        itChunk = shard->GetChunks().First();
    else
        itChunk = shard->GetChunks().Next(itChunk);
    
    if (itChunk == NULL)
    {
        lastResult = NULL;
        Call(onComplete);
        return;
    }

    if ((*itChunk)->GetChunkState() == StorageChunk::Written)
    {
        fileChunk = (StorageFileChunk*) (*itChunk);
        chunkName = fileChunk->GetFilename();
        threadPool->Execute(MFUNC(StorageAsyncBulkCursor, AsyncReadFileChunk));
    }
    else if ((*itChunk)->GetChunkState() == StorageChunk::Unwritten)
    {
        fileChunk = (StorageFileChunk*) (*itChunk);
        unwrittenLister.Init(fileChunk, startKey, 0, 0);
        result = new StorageAsyncBulkResult(this);
        result->dataPage = *unwrittenLister.GetDataPage();
        result->onComplete = onComplete;
        lastResult = result;
        // direct callback, maybe yieldTimer would be better
        result->OnComplete();        
    }
    else if ((*itChunk)->GetChunkState() == StorageChunk::Serialized)
    {
        memoChunk = (StorageMemoChunk*) (*itChunk);
        memoLister.Init(memoChunk, startKey, 0, 0);
        result = new StorageAsyncBulkResult(this);
        result->dataPage = *memoLister.GetDataPage();
        result->onComplete = onComplete;
        lastResult = result;
        // direct callback, maybe yieldTimer would be better
        result->OnComplete();
    }
}

void StorageAsyncBulkCursor::AsyncReadFileChunk()
{
    StorageChunkReader      reader;
    StorageDataPage*        dataPage;
    StorageAsyncBulkResult* result;
    Callable                onNextChunk = MFUNC(StorageAsyncBulkCursor, OnNextChunk);
    
    reader.Open(chunkName, MAX_PRELOAD_THRESHOLD);
    
    lastResult = NULL;
    result = new StorageAsyncBulkResult(this);
    dataPage = reader.FirstDataPage();
    while (dataPage != NULL)
    {
        if (env->shuttingDown)
        {
            // abort cursor
            return;
        }
        
        // rate control
        while (lastResult != NULL || env->yieldThreads || env->asyncGetThread->GetNumPending() > 0)
        {
            MSleep(1);
        }

        TransferDataPage(result, dataPage);
        OnResult(result);
        
        result = new StorageAsyncBulkResult(this);
        dataPage = reader.NextDataPage();
    }
    
    OnResult(result);
    IOProcessor::Complete(&onNextChunk);
}

void StorageAsyncBulkCursor::TransferDataPage(StorageAsyncBulkResult* result, 
 StorageDataPage* dataPage)
{
    // TODO: zero-copy
    result->dataPage = *dataPage;
}

void StorageAsyncBulkCursor::OnResult(StorageAsyncBulkResult* result)
{
    Callable    callable;
    
    result->onComplete = onComplete;
    callable = MFunc<StorageAsyncBulkResult, &StorageAsyncBulkResult::OnComplete>(result);
    IOProcessor::Complete(&callable);
}

void StorageAsyncBulkCursor::Clear()
{
    // TODO:
}
