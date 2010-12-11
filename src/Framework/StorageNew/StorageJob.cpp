#include "StorageJob.h"
#include "StorageChunkWriter.h"

StorageWriteChunkJob::StorageWriteChunkJob(StorageChunkFile* fileChunk_)
{
    fileChunk = fileChunk_;
}

void StorageWriteChunkJob::Execute()
{
    StorageChunkWriter writer;

    writer.Write(fileChunk);
}
