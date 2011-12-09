#include "StorageAsyncGet.h"
#include "StorageFileChunk.h"
#include "StoragePageCache.h"
#include "StorageShard.h"
#include "StorageEnvironment.h"
#include "System/IO/IOProcessor.h"

StorageAsyncGet::StorageAsyncGet()
{
    lastLoadedPage = NULL;
    ret = false;
    completed = false;
    skipMemoChunk = false;
}

StorageChunk** StorageAsyncGet::GetChunkIterator(StorageShard* shard)
{
	StorageChunk**	itChunk;

	itChunk = NULL;
    if (chunkID == 0)
    {
        // start from the newest shard
        return shard->GetChunks().Last();
    }
    else
    {
        // check if the chunk still exists
        FOREACH (itChunk, shard->GetChunks())
        {
            if ((*itChunk)->GetChunkID() == chunkID)
                break;
        }
        
        // chunk was deleted, restart
        if (itChunk == NULL)
        {
            return shard->GetChunks().Last();
        }
    }

	return itChunk;
}

// This function is executed in the main thread
void StorageAsyncGet::ExecuteAsyncGet()
{
    StorageShard*               shard;
    StorageChunk**              itChunk;
    StorageChunk::ChunkState    chunkState;

    // check if the shard still exists
    shard = env->GetShard(contextID, shardID);
    if (!shard)
    {
        itChunk = NULL;
        completed = true;
        goto complete;
    }

	// NULL means the chunk was deleted
	itChunk = GetChunkIterator(shard);
	if (itChunk == NULL)
		goto complete;

	// only filechunks' pages need to be set 
    chunkState = (*itChunk)->GetChunkState();
    if (chunkState == StorageChunk::Written)
        SetLastLoadedPage((StorageFileChunk*) (*itChunk));

    stage = START;
    while (itChunk != NULL)
    {
        // When we return from async page loading, the chunk might already
        // be deleted, so we don't store the pointer, but its ID instead
        chunkID = (*itChunk)->GetChunkID();

		chunkState = (*itChunk)->GetChunkState();
		if (chunkState == StorageChunk::Written)
	        SetupLoaderFileChunk((StorageFileChunk*) (*itChunk));

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

void StorageAsyncGet::SetLastLoadedPage(StorageFileChunk* fileChunk)
{
    if (lastLoadedPage == NULL)
        return;

    if (stage == BLOOM_PAGE)
    {
        if (fileChunk->bloomPage == NULL)
        {
            fileChunk->SetBloomPage((StorageBloomPage*) lastLoadedPage);
            lastLoadedPage = NULL;
        }
    }
    else if (stage == INDEX_PAGE)
    {
        if (fileChunk->indexPage == NULL)
        {
            fileChunk->SetIndexPage((StorageIndexPage*) lastLoadedPage);
            lastLoadedPage = NULL;
        }
    }
    else if (stage == DATA_PAGE)
    {
        if (fileChunk->dataPages == NULL)
        {
            // the fileChunk must have been unloaded and reloaded, go back to INDEX_PAGE stage
            stage = INDEX_PAGE;
        }
        else if (fileChunk->dataPages[index] == NULL)
        {
            fileChunk->SetDataPage((StorageDataPage*) lastLoadedPage);
            lastLoadedPage = NULL;
        }
    }

    if (lastLoadedPage != NULL)
    {
        delete lastLoadedPage;
        lastLoadedPage = NULL;
    }
}

void StorageAsyncGet::SetupLoaderFileChunk(StorageFileChunk* fileChunk)
{
    if (loaderFileChunk.GetChunkID() == fileChunk->GetChunkID())
        return;

    loaderFileChunk.Close();
    loaderFileChunk.Init();

    loaderFileChunk.useCache = false;
    loaderFileChunk.SetFilename(fileChunk->GetFilename());
    // it is safe to shallow copy headerPage
    loaderFileChunk.headerPage = fileChunk->headerPage;
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

    if (stage == BLOOM_PAGE)
        lastLoadedPage = loaderFileChunk.AsyncLoadBloomPage();
    else if (stage == INDEX_PAGE)
        lastLoadedPage = loaderFileChunk.AsyncLoadIndexPage();
    else if (stage == DATA_PAGE)
        lastLoadedPage = loaderFileChunk.AsyncLoadDataPage(index, offset);
    
    asyncGet = MFUNC(StorageAsyncGet, ExecuteAsyncGet);
    IOProcessor::Complete(&asyncGet);
}
