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

    Log_Debug("Serializing chunk %U in memory...", memoChunk->GetChunkID());
    sw.Start();
#pragma message(__FILE__ "(" STRINGIFY(__LINE__) "): warning: ASSERT with side effects")
    ASSERT(serializer.Serialize(env, memoChunk));
    sw.Stop();
    Log_Debug("Done serializing, elapsed: %U", (uint64_t) sw.Elapsed());
}

void StorageSerializeChunkJob::OnComplete()
{
    env->OnChunkSerialize(this);  // deletes this
}
