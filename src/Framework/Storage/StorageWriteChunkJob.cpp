#include "StorageWriteChunkJob.h"
#include "System/Stopwatch.h"
#include "StorageEnvironment.h"
#include "StorageChunkWriter.h"

StorageWriteChunkJob::StorageWriteChunkJob(StorageEnvironment* env_, StorageFileChunk* writeChunk_)
{
    env = env_;
    writeChunk = writeChunk_;
}

void StorageWriteChunkJob::Execute()
{
    StorageChunkWriter  writer;
    Stopwatch           sw;
    bool                ret;

    Log_Debug("Writing chunk %U to file...", writeChunk->GetChunkID());
    sw.Start();
    if (!env->shuttingDown)
    {
        ret = writer.Write(env, writeChunk);
        if (!env->shuttingDown)
            ASSERT(ret);
    }
    sw.Stop();
    Log_Debug("Chunk %U written, elapsed: %U, size: %s, bps: %sB/s",
     writeChunk->GetChunkID(),
     (uint64_t) sw.Elapsed(), HUMAN_BYTES(writeChunk->GetSize()), 
     HUMAN_BYTES((uint64_t)(writeChunk->GetSize() / (sw.Elapsed() / 1000.0))));
}

void StorageWriteChunkJob::OnComplete()
{
    env->OnChunkWrite(this);  // deletes this
}
