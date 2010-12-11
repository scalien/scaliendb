#include "StorageJob.h"
#include "StorageChunkWriter.h"

StorageWriteChunkJob::StorageWriteChunkJob(StorageFileChunk* fileChunk_)
{
    fileChunk = fileChunk_;
}

void StorageWriteChunkJob::Execute()
{
    StorageChunkWriter writer;

    writer.Write(fileChunk);
}
