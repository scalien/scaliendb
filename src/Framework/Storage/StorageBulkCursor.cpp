#include "StorageBulkCursor.h"
#include "StorageEnvironment.h"
#include "StoragePageCache.h"

StorageBulkCursor::StorageBulkCursor() :
 dataPage(NULL, 0)
{
    contextID = 0;
    shardID = 0;
    chunkID = 0;
    shard = NULL;
    env = NULL;
    isLast = false;
}

StorageBulkCursor::~StorageBulkCursor()
{
    env->DecreaseNumCursors();
}

StorageKeyValue* StorageBulkCursor::First()
{
    StorageChunk*       chunk;
    StorageChunk**      itChunk;

    itChunk = shard->chunks.First();
    
    if (itChunk == NULL)
        chunk = shard->GetMemoChunk();
    else
        chunk = *itChunk;

    assert(chunk != NULL);
    
    chunkID = chunk->GetChunkID();
//    Log_Debug("Iterating chunk %U", chunkID);
    
    return FromNextBunch(chunk);
}

StorageKeyValue* StorageBulkCursor::Next(StorageKeyValue* it)
{
    StorageKeyValue*    kv;
    StorageChunk*       chunk;
    StorageChunk**      itChunk;

    kv = dataPage.Next((StorageFileKeyValue*) it);
    
    if (kv != NULL)
    {
        assert(kv->GetKey().GetBuffer()[0] != 0);
        return kv;
    }
    
    if (!env->ShardExists(contextID, shardID))
        return NULL;
        
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
        chunk = *itChunk;
    }
    assert(chunk != NULL);

    chunkID = chunk->GetChunkID();
//    Log_Debug("Iterating chunk %U", chunkID);
    
    kv = FromNextBunch(chunk);
    return kv;
}

void StorageBulkCursor::SetLast(bool isLast_)
{
    isLast = isLast_;
}

ReadBuffer StorageBulkCursor::GetNextKey()
{
    return ReadBuffer(nextKey);
}

void StorageBulkCursor::SetNextKey(ReadBuffer key)
{
    nextKey.Write(key);
}

void StorageBulkCursor::AppendKeyValue(StorageKeyValue* kv)
{
    dataPage.Append(kv);
}

void StorageBulkCursor::FinalizeKeyValues()
{
    dataPage.Finalize();
}

StorageKeyValue* StorageBulkCursor::FromNextBunch(StorageChunk* chunk)
{
    StorageChunk**      itChunk;

    while (true)
    {
        if (!isLast)
        {
            dataPage.Reset();
            chunk->NextBunch(*this, shard);
            if (dataPage.First())
                return dataPage.First();
            else
                continue;
        }
        
        // go to next chunk
        FOREACH(itChunk, shard->chunks)
        {
            if ((*itChunk)->GetChunkID() == chunkID)
            {
//                Log_Debug("Cursor next chunk");
                itChunk = shard->chunks.Next(itChunk);
                isLast = false;
                nextKey.Clear();
                break;
            }
        }
        
        if (itChunk)
        {
            chunk = *itChunk;
            chunkID = chunk->GetChunkID();
//            Log_Message("Iterating chunk %U", chunkID);
        }
        else
        {
            if (shard->GetMemoChunk()->GetChunkID() == chunkID)
                return NULL; // end of iteration
            
            // iterate memoChunk
            chunk = shard->GetMemoChunk();
            chunkID = chunk->GetChunkID();
//            Log_Message("Iterating chunk %U", chunkID);
        }
        
        dataPage.Reset();
    }
}

void StorageBulkCursor::SetEnvironment(StorageEnvironment* env_)
{
    env = env_;
}

void StorageBulkCursor::SetShard(uint64_t contextID_, uint64_t shardID_)
{
    contextID = contextID_;
    shardID = shardID_;
    shard = env->GetShard(contextID, shardID);
    assert(shard);
}
