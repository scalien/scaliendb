#include "StorageCommitJob.h"
#include "StorageEnvironment.h"

StorageCommitJob::StorageCommitJob(StorageEnvironment* env_,
 StorageLogSegment* logSegment_, Callable onCommit_)
{
    env = env_;
    logSegment = logSegment_;
    onCommit = onCommit_;
}

void StorageCommitJob::Execute()
{
    startTime = NowClock();
    logSegment->Commit();
}

void StorageCommitJob::OnComplete()
{
    env->OnCommit(this); // deletes this
}
