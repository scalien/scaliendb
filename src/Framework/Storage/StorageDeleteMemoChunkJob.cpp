#include "StorageDeleteMemoChunkJob.h"
#include "StorageMemoChunk.h"

StorageDeleteMemoChunkJob::StorageDeleteMemoChunkJob(StorageMemoChunk* chunk_)
{
    chunk = chunk_;
}

void StorageDeleteMemoChunkJob::Execute()
{
    Log_Debug("Deleting in-memory chunk %U", chunk->GetChunkID());
    delete chunk;
}

void StorageDeleteMemoChunkJob::OnComplete()
{
    delete this;
}
