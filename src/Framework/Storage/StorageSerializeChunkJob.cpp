#include "StorageSerializeChunkJob.h"
#include "System/Stopwatch.h"
#include "StorageEnvironment.h"
#include "StorageChunkSerializer.h"

StorageSerializeChunkJob::StorageSerializeChunkJob(StorageEnvironment* env_, StorageMemoChunk* memoChunk_)
{
    env = env_;
    memoChunk = memoChunk_;
}

void StorageSerializeChunkJob::Execute()
{
    StorageChunkSerializer  serializer;
    Stopwatch               sw;
    bool                    ret;

    Log_Debug("Serializing chunk %U in memory...", memoChunk->GetChunkID());
    sw.Start();
    ret = serializer.Serialize(env, memoChunk);
    ASSERT(ret);
    sw.Stop();
    Log_Debug("Done serializing, elapsed: %U", (uint64_t) sw.Elapsed());
}

void StorageSerializeChunkJob::OnComplete()
{
    env->OnChunkSerialize(this);  // deletes this
}
