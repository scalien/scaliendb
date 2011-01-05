#include "StorageBulkCursor.h"
#include "StorageEnvironment.h"
#include "StoragePageCache.h"

StorageCursorBunch::StorageCursorBunch()
{
    isLast = false;
}

StorageCursorBunch::~StorageCursorBunch()
{
    keyValues.DeleteTree();
}

StorageKeyValue* StorageCursorBunch::First()
{
    return keyValues.First();
}

StorageKeyValue* StorageCursorBunch::Next(StorageKeyValue* it)
{
    return keyValues.Next((StorageFileKeyValue*) it);
}

ReadBuffer StorageCursorBunch::GetNextKey()
{
    return ReadBuffer(nextKey);
}

bool StorageCursorBunch::IsLast()
{
    return isLast;
}

void StorageCursorBunch::Reset()
{
    keyValues.DeleteTree();
    buffer.Reset();
    isLast = false;
    nextKey.Reset();
}

StorageBulkCursor::StorageBulkCursor()
{
    chunkID = 0;
    shard = NULL;
    env = NULL;
}

StorageBulkCursor::~StorageBulkCursor()
{
}

StorageKeyValue* StorageBulkCursor::First()
{
    StorageChunk*       chunk;
    StorageChunk**      itChunk;

    StoragePageCache::TryUnloadPages(env->GetStorageConfig());

    itChunk = shard->chunks.First();
    
    if (itChunk == NULL)
        return NULL;

    chunk = *itChunk;
    assert(chunk != NULL);
    
    chunkID = chunk->GetChunkID();
    Log_Message("Iterating chunk %U", chunkID);
    
    return FromNextBunch(chunk);
}

StorageKeyValue* StorageBulkCursor::Next(StorageKeyValue* it)
{
    StorageKeyValue*    kv;
    StorageChunk*       chunk;
    StorageChunk**      itChunk;

    kv = bunch.Next(it);
    
    if (kv != NULL)
        return kv;
    
    // TODO: what if shard has been deleted?
    FOREACH(itChunk, shard->chunks)
    {
        if ((*itChunk)->GetChunkID() == chunkID)
            break;
    }
    
    if (itChunk == NULL)
    {
        chunk = shard->GetMemoChunk();
    }
    else
    {        
        assert(itChunk != NULL); // TODO: what if chunk has been deleted?
        chunk = *itChunk;
    }
    assert(chunk != NULL);

    chunkID = chunk->GetChunkID();
    Log_Message("Iterating chunk %U", chunkID);
    
    return FromNextBunch(chunk);
}

StorageKeyValue* StorageBulkCursor::FromNextBunch(StorageChunk* chunk)
{
    StorageChunk**      itChunk;

    StoragePageCache::TryUnloadPages(env->GetStorageConfig());
    bunch.keyValues.DeleteTree();

    while (true)
    {
        if (!bunch.IsLast())
        {
            chunk->NextBunch(bunch, shard);
            if (bunch.First())
                return bunch.First();
            else
                continue;
        }
        
        // go to next chunk
        FOREACH(itChunk, shard->chunks)
        {
            if ((*itChunk)->GetChunkID() == chunkID)
            {
                itChunk = shard->chunks.Next(itChunk);
                break;
            }
        }
        
        if (itChunk)
        {
            chunk = *itChunk;
            chunkID = chunk->GetChunkID();
            Log_Message("Iterating chunk %U", chunkID);
        }
        else
        {
            if (shard->GetMemoChunk()->GetChunkID() == chunkID)
                return NULL; // end of iteration
            
            // iterate memoChunk
            chunk = shard->GetMemoChunk();
            chunkID = chunk->GetChunkID();
            Log_Message("Iterating chunk %U", chunkID);
        }
        
        bunch.Reset();
    }
}

void StorageBulkCursor::SetEnvironment(StorageEnvironment* env_)
{
    env = env_;
}

void StorageBulkCursor::SetShard(StorageShard* shard_)
{
    shard = shard_;
}
