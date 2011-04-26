#include "StorageDeleteFileChunkJob.h"
#include "System/Stopwatch.h"
#include "System/FileSystem.h"
#include "StorageFileChunk.h"

StorageDeleteFileChunkJob::StorageDeleteFileChunkJob(StorageFileChunk* chunk_)
{
    chunk = chunk_;
}

void StorageDeleteFileChunkJob::Execute()
{
    Stopwatch   sw;
    
    Log_Debug("Deleting chunk %U", chunk->GetChunkID());
    sw.Start();
    
    FS_Delete(chunk->GetFilename().GetBuffer());

    delete chunk;
    delete this;

    sw.Stop();
    Log_Debug("Deleted, elapsed: %U", (uint64_t) sw.Elapsed());
}

void StorageDeleteFileChunkJob::OnComplete()
{
    delete this;
}
