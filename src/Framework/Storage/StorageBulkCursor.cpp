#include "StorageBulkCursor.h"
#include "StorageEnvironment.h"

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


StorageKeyValue* StorageBulkCursor::First()
{
    StorageChunk*       chunk;
    StorageChunk**      itChunk;

    itChunk = shard->chunks.First();
    
    if (itChunk == NULL)
        return NULL;

    chunk = *itChunk;
    assert(chunk != NULL);
    
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
    
    assert(itChunk != NULL); // TODO: what if chunk has been deleted?
    chunk = *itChunk;
    assert(chunk != NULL);
    
    return FromNextBunch(chunk);
}

StorageKeyValue* StorageBulkCursor::FromNextBunch(StorageChunk* chunk)
{
    StorageChunk**      itChunk;

    while (true)
    {
        if (!bunch.IsLast())
        {
            chunk->NextBunch(bunch);
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
            chunk = *itChunk;
        else
        {
            if (shard->GetMemoChunk()->GetChunkID() == chunkID)
                return NULL; // end of iteration
            
            // iterate memoChunk
            chunk = shard->GetMemoChunk();
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
