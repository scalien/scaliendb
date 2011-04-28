#include "StorageMergeChunkJob.h"
#include "System/Stopwatch.h"
#include "StorageEnvironment.h"
#include "StorageChunkMerger.h"

StorageMergeChunkJob::StorageMergeChunkJob(StorageEnvironment* env_,
 uint64_t contextID_, uint64_t shardID_,
 List<StorageFileChunk*>& inputChunks_,
 StorageFileChunk* mergeChunk_,
 ReadBuffer firstKey_, ReadBuffer lastKey_)
{
    StorageFileChunk**  itChunk;
    
    env = env_;
    contextID = contextID_;
    shardID = shardID_;
    
    FOREACH(itChunk, inputChunks_)
        inputChunks.Append(*itChunk);

    firstKey.Write(firstKey_);
    lastKey.Write(lastKey_);
    mergeChunk = mergeChunk_;
}

void StorageMergeChunkJob::Execute()
{
    bool                ret;
    Buffer*             filename;
    StorageFileChunk**  itInputChunk;
    StorageChunkMerger  merger;
    List<Buffer*>       filenames;
    Stopwatch           sw;

    FOREACH (itInputChunk, inputChunks)
    {
        filename = &(*itInputChunk)->GetFilename();
        filenames.Add(filename);
    }
    
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
}

void StorageMergeChunkJob::OnComplete()
{
    env->OnChunkMerge(this); // deletes this
    // delete this;
}
