#include "StorageAsyncGet.h"
#include "StorageFileChunk.h"
#include "StoragePageCache.h"
#include "StorageShard.h"
#include "System/IO/IOProcessor.h"

StorageAsyncGet::StorageAsyncGet()
{
    lastLoadedPage = NULL;
    ret = false;
    completed = false;
    skipMemoChunk = false;
}

// This function is executed in the main thread
void StorageAsyncGet::ExecuteAsyncGet()
{
    StorageFileChunk*   fileChunk;

    if (itChunk == NULL)
        goto complete;
    
    fileChunk = (StorageFileChunk*) (*itChunk);
    if (stage == BLOOM_PAGE)
    {
        if (lastLoadedPage != NULL)
        {
            ASSERT(fileChunk->bloomPage == NULL);
            fileChunk->bloomPage = (StorageBloomPage*) lastLoadedPage;
            fileChunk->isBloomPageLoading = false;
        }
    }
    else if (stage == INDEX_PAGE)
    {
        if (lastLoadedPage != NULL)
        {
            ASSERT(fileChunk->indexPage == NULL);
            fileChunk->indexPage = (StorageIndexPage*) lastLoadedPage;
            if (fileChunk->dataPages == NULL)
                fileChunk->AllocateDataPageArray();
            fileChunk->isIndexPageLoading = false;
        }
    }
    else if (stage == DATA_PAGE)
    {
        if (lastLoadedPage != NULL)
        {
            // TODO: FIXME:
//            ASSERT(fileChunk->dataPages[index] == NULL);
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

complete:
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
