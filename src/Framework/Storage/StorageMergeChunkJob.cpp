#include "StorageMergeChunkJob.h"
#include "System/Stopwatch.h"
#include "StorageEnvironment.h"
#include "StorageChunkMerger.h"

StorageMergeChunkJob::StorageMergeChunkJob(StorageEnvironment* env_,
 List<Buffer*>& filenames_, StorageFileChunk* mergeChunk_,
 ReadBuffer firstKey_, ReadBuffer lastKey_)
{
    Buffer**    itFilename;
    Buffer*     filename;
    
    env = env_;

    FOREACH (itFilename, filenames_)
    {
        filename = new Buffer(**itFilename);
        filenames.Append(filename);
    }

    firstKey.Write(firstKey_);
    lastKey.Write(lastKey_);
    mergeChunk = mergeChunk_;
}

void StorageMergeChunkJob::Execute()
{
    bool                ret;
    StorageChunkMerger  merger;
    Buffer**            itFilename;
    Stopwatch           sw;
    
    Log_Debug("Merging %u chunks into chunk %U...",
     filenames.GetLength(),
     mergeChunk->GetChunkID());
    sw.Start();
    ret = merger.Merge(env, filenames, mergeChunk, firstKey, lastKey);
    sw.Stop();
    if (ret)
    {
        Log_Debug("Done merging chunk %U, elapsed: %U, size: %s, bps: %sB/s",
         mergeChunk->GetChunkID(),
         (uint64_t) sw.Elapsed(), HUMAN_BYTES(mergeChunk->GetSize()), 
         HUMAN_BYTES(mergeChunk->GetSize() / (sw.Elapsed() / 1000.0)));
    }

    FOREACH_FIRST (itFilename, filenames)
    {
        delete *itFilename;
        filenames.Remove(itFilename);
    }
}

void StorageMergeChunkJob::OnComplete()
{
    env->OnChunkMerge(this); // deletes this
    // delete this;
}
