#include "StorageDeleteMemoChunkJob.h"
#include "StorageMemoChunk.h"

StorageDeleteMemoChunkJob::StorageDeleteMemoChunkJob(StorageMemoChunk* chunk_)
{
    chunk = chunk_;
    chunkID = chunk->GetChunkID();
}

void StorageDeleteMemoChunkJob::Execute()
{
    Log_Debug("Deleting in-memory chunk %U", chunkID);
    delete chunk;
    Log_Debug("Deleting done, chunkID %U", chunkID);
}

void StorageDeleteMemoChunkJob::OnComplete()
{
    Log_Debug("DeletingMemoChunkJob OnComplete start, chunk %U", chunkID);
    delete this;
    Log_Debug("DeletingMemoChunkJob OnComplete end");
}
