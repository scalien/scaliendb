#include "StorageJob.h"
#include "StorageChunkWriter.h"
#include "StorageChunkSerializer.h"
#include "System/IO/IOProcessor.h"

StorageSerializeChunkJob::StorageSerializeChunkJob(StorageMemoChunk* memoChunk_, Callable* onComplete_)
{
    memoChunk = memoChunk_;
    onComplete = onComplete_;
}

void StorageSerializeChunkJob::Execute()
{
    StorageChunkSerializer serializer;

    Log_Message("Serializing chunk %U in memory...", memoChunk->GetChunkID());
    serializer.Serialize(memoChunk);
    Log_Message("Done.");
    
    Callable* c = onComplete;
    delete this;
    IOProcessor::Complete(c);
}

StorageWriteChunkJob::StorageWriteChunkJob(StorageFileChunk* fileChunk_, Callable* onComplete_)
{
    fileChunk = fileChunk_;
    onComplete = onComplete_;
}

void StorageWriteChunkJob::Execute()
{
    StorageChunkWriter writer;

    Log_Message("Writing chunk %U to file...", fileChunk->GetChunkID());
    writer.Write(fileChunk);
    Log_Message("Done.");

    Callable* c = onComplete;
    delete this;
    IOProcessor::Complete(c);
}
