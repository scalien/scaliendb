#include "StorageDeleteMemoChunkJob.h"
#include "StorageMemoChunk.h"

StorageDeleteMemoChunkJob::StorageDeleteMemoChunkJob(StorageMemoChunk* chunk_)
{
    chunk = chunk_;
}

void StorageDeleteMemoChunkJob::Execute()
{
    delete chunk;
}

void StorageDeleteMemoChunkJob::OnComplete()
{
    delete this;
}
