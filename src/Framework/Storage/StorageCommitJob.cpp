#include "StorageCommitJob.h"
#include "StorageEnvironment.h"

StorageCommitJob::StorageCommitJob(StorageEnvironment* env_, StorageLogSegment* logSegment_)
{
    env = env_;
    logSegment = logSegment_;
}

void StorageCommitJob::Execute()
{
    logSegment->Commit();
}

void StorageCommitJob::OnComplete()
{
    env->OnCommit(this); // deletes this
}
