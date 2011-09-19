#include "StorageBulkCursor.h"
#include "StorageEnvironment.h"
#include "StoragePageCache.h"

StorageBulkCursor::StorageBulkCursor()
 : dataPage(NULL, 0)
{
    blockShard = false;
    shard = NULL;
    isLast = false;
    contextID = 0;
    shardID = 0;
    chunkID = 0;
    logSegmentID = 0;
    logCommandID = 0;
    env = NULL;
}

StorageBulkCursor::~StorageBulkCursor()
{
    env->DecreaseNumCursors();
}

void StorageBulkCursor::SetEnvironment(StorageEnvironment* env_)
{
    env = env_;
}

void StorageBulkCursor::SetOnBlockShard(Callable onBlockShard_)
{
    blockShard = true;
    onBlockShard = onBlockShard_;
}

void StorageBulkCursor::SetShard(uint64_t contextID_, uint64_t shardID_)
{
    contextID = contextID_;
    shardID = shardID_;
    shard = env->GetShard(contextID, shardID);
    ASSERT(shard);
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

    ASSERT(chunk != NULL);
    
    chunkID = chunk->GetChunkID();
    logSegmentID = chunk->GetMaxLogSegmentID();
    logCommandID = chunk->GetMaxLogCommandID();
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
        ASSERT(kv->GetKey().GetLength() > 0);
        return kv;
    }
    
    if (!env->ShardExists(contextID, shardID))
        return NULL;
        
    FOREACH (itChunk, shard->chunks)
    {
        if ((*itChunk)->GetChunkID() == chunkID)
            break;
        if (((*itChunk)->GetMaxLogSegmentID() == logSegmentID && (*itChunk)->GetMaxLogCommandID() > logCommandID) || (*itChunk)->GetMaxLogSegmentID() > logSegmentID)
        {
            // chunk has been deleted, clear nextKey to read the merged chunk from the beginning
            Log_Debug("chunk has been deleted, clear nextKey to read the merged chunk from the beginning");
            nextKey.Clear();
            break;
        }
    }
    
    if (itChunk == NULL && blockShard)
    {
        if (shard->GetMemoChunk()->GetSize() > 0)
        {
            Log_Debug("Pushing memo chunk1");
#pragma message(__FILE__ "(" STRINGIFY(__LINE__) "): warning: ASSERT with side effects") 
            ASSERT(env->PushMemoChunk(contextID, shardID));
            chunk = *(shard->chunks.Last()); // this is the memo chunk we just pushed
            if (chunk->GetSize() < STORAGE_MEMO_BUNCH_GRAN)
                Call(onBlockShard);
        }
        else
        {
            Call(onBlockShard);
            return NULL;
        }
    }
    else
    {
        if (itChunk == NULL)
            chunk = shard->GetMemoChunk();
        else
            chunk = *itChunk;
    }
    ASSERT(chunk != NULL);

    chunkID = chunk->GetChunkID();
    logSegmentID = chunk->GetMaxLogSegmentID();
    logCommandID = chunk->GetMaxLogCommandID();
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
        FOREACH (itChunk, shard->chunks)
        {
            if ((*itChunk)->GetChunkID() == chunkID)
            {
//                Log_Debug("Cursor next chunk");
                itChunk = shard->chunks.Next(itChunk);
                isLast = false;
                nextKey.Clear();
                break;
            }
            if (((*itChunk)->GetMaxLogSegmentID() == logSegmentID && (*itChunk)->GetMaxLogCommandID() > logCommandID) || (*itChunk)->GetMaxLogSegmentID() > logSegmentID)
            {
                Log_Debug("chunk has been deleted, clear nextKey to read the merged chunk from the beginning");
                isLast = false;
                nextKey.Clear();
                break;
            }
        }
        
        if (itChunk)
            chunk = *itChunk;
        else
        {
            if (shard->GetMemoChunk()->GetChunkID() == chunkID)
            {
                if (blockShard)
                    Call(onBlockShard);
                return NULL; // end of iteration
            }

            if (blockShard)
            {
                if (shard->GetMemoChunk()->GetSize() > 0)
                {
                    Log_Debug("Pushing memo chunk2");
#pragma message(__FILE__ "(" STRINGIFY(__LINE__) "): warning: ASSERT with side effects")
                    ASSERT(env->PushMemoChunk(contextID, shardID));
                    chunk = *(shard->chunks.Last()); // this is the memo chunk we just pushed
                    if (chunk->GetSize() < STORAGE_MEMO_BUNCH_GRAN)
                        Call(onBlockShard);
                }
                else
                {
                    Call(onBlockShard);
                    return NULL;
                }
            }
            else
                chunk = shard->GetMemoChunk();
        }
        chunkID = chunk->GetChunkID();
        logSegmentID = chunk->GetMaxLogSegmentID();
        logCommandID = chunk->GetMaxLogCommandID();
        
        dataPage.Reset();
    }
}
