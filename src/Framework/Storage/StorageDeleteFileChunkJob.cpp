#include "StorageDeleteFileChunkJob.h"
#include "System/Stopwatch.h"
#include "System/FileSystem.h"
#include "StorageFileChunk.h"
#include "StorageFileDeleter.h"

StorageDeleteFileChunkJob::StorageDeleteFileChunkJob(StorageFileChunk* chunk_)
{
    chunk = chunk_;
}

StorageDeleteFileChunkJob::~StorageDeleteFileChunkJob()
{
    // make sure chunk is deleted when the job is terminated without Execute() being called
    delete chunk;
}

void StorageDeleteFileChunkJob::Execute()
{
    Stopwatch   sw;
    Buffer      filename;
    
    Log_Message("Deleting file chunk %U from disk", chunk->GetChunkID());
    sw.Start();
    
    filename.Write(chunk->GetFilename());
    delete chunk;
    chunk = NULL;

    StorageFileDeleter::Delete(filename.GetBuffer());

    sw.Stop();
    Log_Debug("Deleted, elapsed: %U", (uint64_t) sw.Elapsed());
}

void StorageDeleteFileChunkJob::OnComplete()
{
    delete this;
}
