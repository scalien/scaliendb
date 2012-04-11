#include "StorageDeleteFileChunkJob.h"
#include "System/Stopwatch.h"
#include "System/FileSystem.h"
#include "StorageFileChunk.h"
#include "StorageFileDeleter.h"

StorageDeleteFileChunkJob::StorageDeleteFileChunkJob(StorageFileChunk* chunk_)
{
    chunk = chunk_;
}

void StorageDeleteFileChunkJob::Execute()
{
    Stopwatch   sw;
    Buffer      filename;
    
    Log_Debug("Deleting file chunk %U from disk, starting", chunk->GetChunkID());
    sw.Start();
    
    filename.Write(chunk->GetFilename());
    delete chunk;

    StorageFileDeleter::Delete(filename.GetBuffer());

    sw.Stop();
    Log_Debug("Deleted, elapsed: %U", (uint64_t) sw.Elapsed());
}

void StorageDeleteFileChunkJob::OnComplete()
{
    delete this;
}
