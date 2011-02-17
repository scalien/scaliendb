#include "StorageAsyncList.h"
#include "System/ThreadPool.h"
#include "StorageMemoChunkLister.h"
#include "StorageFileChunkLister.h"
#include "StorageShard.h"

StorageAsyncList::StorageAsyncList()
{
    // TODO:
}

void StorageAsyncList::ExecuteAsyncList()
{
    listers = new StorageChunkLister*[shard->GetChunks().GetLength()];
    numListers = 0;
 
    if (stage == START)
    {
        stage = MEMO_CHUNK;
        LoadMemoChunk();
        stage = FILE_CHUNK;
        return;
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
    listers[numListers] = memoLister;
    numListers++;
}

void StorageAsyncList::AsyncLoadFileChunks()
{
    StorageChunk**          itChunk;
    StorageFileChunk*       fileChunk;
    StorageFileChunkLister* fileLister;
    Buffer*                 filename;
    
    FOREACH (itChunk, shard->GetChunks())
    {
        if ((*itChunk)->GetChunkState() != StorageChunk::Written)
            continue;
        fileChunk = (StorageFileChunk*) *itChunk;
        filename = &fileChunk->GetFilename();
        fileLister = new StorageFileChunkLister;
        fileLister->Load(*filename);
        listers[numListers] = fileLister;
        numListers++;
    }
    
    stage = MERGE;
    AsyncMergeResult();
}

void StorageAsyncList::AsyncMergeResult()
{
}

StorageFileKeyValue* StorageAsyncList::Next(ReadBuffer& lastKey)
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