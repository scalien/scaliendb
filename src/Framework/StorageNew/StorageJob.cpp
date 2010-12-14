#include "StorageJob.h"
#include "System/IO/IOProcessor.h"
#include "System/FileSystem.h"
#include "StorageChunkWriter.h"
#include "StorageChunkSerializer.h"
#include "StorageEnvironment.h"

StorageSerializeChunkJob::StorageSerializeChunkJob(StorageMemoChunk* memoChunk_, Callable* onComplete_)
{
    memoChunk = memoChunk_;
    onComplete = onComplete_;
}

void StorageSerializeChunkJob::Execute()
{
    StorageChunkSerializer serializer;

    Log_Message("Serializing chunk %U in memory...", memoChunk->GetChunkID());
    serializer.Serialize(memoChunk);
    
    Callable* c = onComplete;
    delete this;
    IOProcessor::Complete(c);
}

StorageWriteChunkJob::StorageWriteChunkJob(StorageFileChunk* fileChunk_, Callable* onComplete_)
{
    fileChunk = fileChunk_;
    onComplete = onComplete_;
}

void StorageWriteChunkJob::Execute()
{
    StorageChunkWriter writer;

    Log_Message("Writing chunk %U to file...", fileChunk->GetChunkID());
    writer.Write(fileChunk);

    Callable* c = onComplete;
    delete this;
    IOProcessor::Complete(c);
}

StorageArchiveLogSegmentJob::StorageArchiveLogSegmentJob(StorageEnvironment* env_, StorageLogSegment* logSegment_, Callable* onComplete_)
{
    env = env_;
    logSegment = logSegment_;
    onComplete = onComplete_;
}

void StorageArchiveLogSegmentJob::Execute()
{
    Buffer dest;
    
    dest.Write(env->archivePath);
    dest.Appendf("log.%020U", logSegment->GetLogSegmentID());
    dest.NullTerminate();
    
    Log_Message("Archiving log segment %U...", logSegment->GetLogSegmentID());
    FS_Rename(logSegment->filename.GetBuffer(), dest.GetBuffer());

    Callable* c = onComplete;
    delete this;
    IOProcessor::Complete(c);
}

