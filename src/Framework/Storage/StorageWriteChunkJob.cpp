#include "StorageWriteChunkJob.h"
#include "System/Stopwatch.h"
#include "StorageEnvironment.h"
#include "StorageChunkWriter.h"

StorageWriteChunkJob::StorageWriteChunkJob(StorageEnvironment* env_, StorageFileChunk* fileChunk_)
{
    env = env_;
    fileChunk = fileChunk_;
}

void StorageWriteChunkJob::Execute()
{
    StorageChunkWriter  writer;
    Stopwatch           sw;

    Log_Debug("Writing chunk %U to file...", fileChunk->GetChunkID());
    sw.Start();
    writer.Write(env, fileChunk);
    sw.Stop();
    Log_Debug("Chunk %U written, elapsed: %U, size: %s, bps: %sB/s",
     fileChunk->GetChunkID(),
     (uint64_t) sw.Elapsed(), HUMAN_BYTES(fileChunk->GetSize()), 
     HUMAN_BYTES(fileChunk->GetSize() / (sw.Elapsed() / 1000.0)));
}

void StorageWriteChunkJob::OnComplete()
{
    env->OnWriteChunk(this);  // deletes this
}
