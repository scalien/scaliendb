#include "StorageWriteChunkJob.h"
#include "System/Stopwatch.h"
#include "System/FileSystem.h"
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
    ret = writer.Write(env, writeChunk);
    sw.Stop();

    if (writeChunk->writeError)
    {
        // write failed
        Log_Message("Unable to write chunk file %U to disk.", writeChunk->GetChunkID());
        Log_Message("Free disk space: %s", HUMAN_BYTES(FS_FreeDiskSpace(writeChunk->GetFilename().GetBuffer())));
        Log_Message("This should not happen.");
        Log_Message("Possible causes: not enough disk space, software bug...");
        STOP_FAIL(1);
    }

    if (ret)
    {
        Log_Message("Chunk %U written, elapsed: %U, size: %s, bps: %sB/s",
         writeChunk->GetChunkID(),
         (uint64_t) sw.Elapsed(), HUMAN_BYTES(writeChunk->GetSize()), 
         HUMAN_BYTES((uint64_t)(writeChunk->GetSize() / (sw.Elapsed() / 1000.0))));
    }
    else
    {
        Log_Message("Write aborted, chunk %U, elapsed: %U, size: %s, bps: %sB/s, free disk space: %s",
         writeChunk->GetChunkID(),
         (uint64_t) sw.Elapsed(), HUMAN_BYTES(writeChunk->GetSize()),
         HUMAN_BYTES((uint64_t)(writeChunk->GetSize() / (sw.Elapsed() / 1000.0))),
         HUMAN_BYTES(FS_FreeDiskSpace(writeChunk->GetFilename().GetBuffer())));
    }
}

void StorageWriteChunkJob::OnComplete()
{
    env->OnChunkWrite(this);  // deletes this
}
