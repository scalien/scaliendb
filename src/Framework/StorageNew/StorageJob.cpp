#include "StorageJob.h"
#include "StorageChunkWriter.h"
#include "StorageChunkSerializer.h"

StorageSerializeChunkJob::StorageSerializeChunkJob(StorageMemoChunk* memoChunk_)
{
    memoChunk = memoChunk_;
}

void StorageSerializeChunkJob::Execute()
{
    StorageChunkSerializer serializer;

    Log_Message("Serializing chunk %U in memory...", memoChunk->GetChunkID());
    serializer.Serialize(memoChunk);
    Log_Message("Done.");
}

StorageWriteChunkJob::StorageWriteChunkJob(StorageFileChunk* fileChunk_)
{
    fileChunk = fileChunk_;
}

void StorageWriteChunkJob::Execute()
{
    StorageChunkWriter writer;

    Log_Message("Writing chunk %U to file...", fileChunk->GetChunkID());
    writer.Write(fileChunk);
    Log_Message("Done.");
}
