#include "StorageBulkCursor.h"
#include "StorageEnvironment.h"
#include "StoragePageCache.h"

//StorageCursorBunch::StorageCursorBunch()
//{
//    isLast = false;
//}
//
//StorageCursorBunch::~StorageCursorBunch()
//{
//    keyValues.DeleteTree();
//}
//
//StorageKeyValue* StorageCursorBunch::First()
//{
//    return keyValues.First();
//}
//
//StorageKeyValue* StorageCursorBunch::Next(StorageKeyValue* it)
//{
//    return keyValues.Next((StorageFileKeyValue*) it);
//}
//
//ReadBuffer StorageCursorBunch::GetNextKey()
//{
//    return ReadBuffer(nextKey);
//}
//
//bool StorageCursorBunch::IsLast()
//{
//    return isLast;
//}
//
//void StorageCursorBunch::Reset()
//{
//    keyValues.DeleteTree();
//    buffer.Reset();
//    isLast = false;
//    nextKey.Reset();
//}
//
//void StorageCursorBunch::InsertKeyValue(StorageFileKeyValue* kv)
//{
//    // TODO:
//}

StorageBulkCursor::StorageBulkCursor() :
 dataPage(NULL, 0)
{
    chunkID = 0;
    shard = NULL;
    env = NULL;
    isLast = false;
}

StorageBulkCursor::~StorageBulkCursor()
{
    env->numBulkCursors = env->numBulkCursors - 1;
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
    
    chunkID = chunk->GetChunkID();
//    Log_Message("Iterating chunk %U", chunkID);
    
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
//    Log_Message("Iterating chunk %U", chunkID);
    
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
                Log_Debug("Cursor next chunk");
                itChunk = shard->chunks.Next(itChunk);
                isLast = false;
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

void StorageBulkCursor::SetShard(StorageShard* shard_)
{
    shard = shard_;
}
