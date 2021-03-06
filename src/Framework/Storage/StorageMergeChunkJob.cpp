#include "StorageMergeChunkJob.h"
#include "System/Stopwatch.h"
#include "System/FileSystem.h"
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

StorageMergeChunkJob::~StorageMergeChunkJob()
{
    delete mergeChunk;
}

void StorageMergeChunkJob::Execute()
{
    bool                ret;
    Buffer*             filename;
    StorageFileChunk**  itInputChunk;
    StorageChunkMerger  merger;
    List<Buffer*>       filenames;
    Stopwatch           sw;
    char                humanSize[5];
    char                humanElapsed[5];

    FOREACH (itInputChunk, inputChunks)
    {
        filename = &(*itInputChunk)->GetFilename();
        filenames.Add(filename);
    }
    
    Log_Message("Merging %u chunks into chunk %U...",
     filenames.GetLength(),
     mergeChunk->GetChunkID());
    sw.Start();
    ret = merger.Merge(env, filenames, mergeChunk, firstKey, lastKey);
    sw.Stop();

    if (mergeChunk->writeError)
    {
        // write failed
        Log_Message("Unable to write chunk file %U to disk.", mergeChunk->GetChunkID());
        Log_Message("Free disk space: %s", HumanBytes(FS_FreeDiskSpace(mergeChunk->GetFilename().GetBuffer()), humanSize));
        Log_Message("This should not happen.");
        Log_Message("Possible causes: not enough disk space, software bug...");
        STOP_FAIL(1);
    }

    if (ret)
    {
        Log_Message("Done merging chunk %U, elapsed: %U, size: %s, bps: %sB/s",
         mergeChunk->GetChunkID(),
         (uint64_t) sw.Elapsed(), HumanBytes(mergeChunk->GetSize(), humanSize), 
         HumanBytes(BYTE_PER_SEC(mergeChunk->GetSize(), sw.Elapsed()), humanElapsed));
    }
    else
    {
        Log_Message("Merge aborted, chunk %U, elapsed: %U, free disk space: %s",
         mergeChunk->GetChunkID(),
         (uint64_t) sw.Elapsed(),
         HumanBytes(FS_FreeDiskSpace(mergeChunk->GetFilename().GetBuffer()), humanSize));
    } 
}

void StorageMergeChunkJob::OnComplete()
{
    env->OnChunkMerge(this); // deletes this
}
