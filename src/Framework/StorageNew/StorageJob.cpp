#include "StorageJob.h"

StorageWriteChunkJob::StorageWriteChunkJob(StorageChunk* chunk_)
{
    chunk = chunk_;
}

void StorageWriteChunkJob::Execute()
{
    chunk->WriteFile();
}
