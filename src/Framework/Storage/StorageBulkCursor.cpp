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
    blockCounter = 0;
}

StorageBulkCursor::~StorageBulkCursor()
{
    env->DecreaseNumCursors();
}

void StorageBulkCursor::SetEnvironment(StorageEnvironment* env_)
{
    env = env_;
}

void StorageBulkCursor::SetOnBlockShard(Callable onBlockShard_, Callable onUnblockShard_)
{
    blockShard = true;
    onBlockShard = onBlockShard_;
    onUnblockShard = onUnblockShard_;
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

    nextKey.Write(shard->GetFirstKey());
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
    {
        if (blockShard && blockCounter > 0)
        {
            ASSERT(blockCounter == 1);
            blockCounter--;
            Call(onUnblockShard);
        }
        return NULL;
    }

    FOREACH (itChunk, shard->chunks)
    {
        if ((*itChunk)->GetChunkID() == chunkID)
            break;
        if (((*itChunk)->GetMaxLogSegmentID() == logSegmentID && (*itChunk)->GetMaxLogCommandID() > logCommandID) || (*itChunk)->GetMaxLogSegmentID() > logSegmentID)
        {
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
            if (!env->PushMemoChunk(contextID, shardID))
                ASSERT_FAIL();
            chunk = *(shard->chunks.Last()); // this is the memo chunk we just pushed
            if (chunk->GetSize() < STORAGE_MEMO_BUNCH_GRAN)
            {
                if (blockCounter == 0)
                {
                    blockCounter++;
                    Call(onBlockShard);
                }
                else
                {
                    ASSERT(blockCounter == 1);
                }
            }
        }
        else
        {
            if (blockCounter > 0)
            {
                ASSERT(blockCounter == 1);
                blockCounter--;
                Call(onUnblockShard);
            }
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
            //Log_Debug("NextBunch chunkID = %U", chunkID);
            if (dataPage.First())
                return dataPage.First();
            else    
                continue;
        }
        
        if (chunkID != shard->GetMemoChunk()->GetChunkID())
        {
            // go to next chunk
            FOREACH (itChunk, shard->chunks)
            {
                if ((*itChunk)->GetChunkID() == chunkID)
                {
                    Log_Debug("Cursor next chunk current chunkID = %U", chunkID);
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
        }
        else
            itChunk = NULL;
        
        if (itChunk)
            chunk = *itChunk;
        else
        {
            if (shard->GetMemoChunk()->GetChunkID() == chunkID)
            {
                if (blockShard)
                {
                    if (blockCounter > 0)
                    {
                        ASSERT(blockCounter == 1);
                        blockCounter--;
                        Call(onUnblockShard);
                    }

                }
                Log_Debug("End of iteration");
                return NULL; // end of iteration
            }

            if (blockShard)
            {
                if (shard->GetMemoChunk()->GetSize() > 0)
                {
                    Log_Debug("Pushing memo chunk2");
                    if (!env->PushMemoChunk(contextID, shardID))
                        ASSERT_FAIL();
                    chunk = *(shard->chunks.Last()); // this is the memo chunk we just pushed
                    if (chunk->GetSize() < STORAGE_MEMO_BUNCH_GRAN)
                    {
                        if (blockCounter == 0)
                        {
                            blockCounter++;
                            Call(onBlockShard);
                        }
                        else
                        {
                            ASSERT(blockCounter == 1);
                        }
                    }
                }
                else
                {
                    if (blockCounter > 0)
                    {
                        ASSERT(blockCounter == 1);
                        blockCounter--;
                        Call(onUnblockShard);
                    }
                    return NULL;
                }
            }
            else
                chunk = shard->GetMemoChunk();
        }
        chunkID = chunk->GetChunkID();
        logSegmentID = chunk->GetMaxLogSegmentID();
        logCommandID = chunk->GetMaxLogCommandID();
        Log_Debug("Next chunk chunkID = %U", chunkID);
        dataPage.Reset();
    }
}
