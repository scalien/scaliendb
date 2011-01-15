#include "StorageJob.h"
#include "System/IO/IOProcessor.h"
#include "System/FileSystem.h"
#include "System/Config.h"
#include "StorageChunkWriter.h"
#include "StorageChunkSerializer.h"
#include "StorageChunkMerger.h"
#include "StorageEnvironment.h"

StorageCommitJob::StorageCommitJob(StorageLogSegment* logSegment_, Callable* onComplete_)
{
    logSegment = logSegment_;
    onComplete = onComplete_;
}

void StorageCommitJob::Execute()
{
    logSegment->Commit();

    Callable* c = onComplete;
    delete this;
    IOProcessor::Complete(c);
}

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
    Log_Message("Done serializing.", memoChunk->GetChunkID());
    
    Callable* c = onComplete;
    delete this;
    IOProcessor::Complete(c);
}

StorageWriteChunkJob::StorageWriteChunkJob(StorageEnvironment* env_, StorageFileChunk* fileChunk_, Callable* onComplete_)
{
    env = env_;
    fileChunk = fileChunk_;
    onComplete = onComplete_;
}

void StorageWriteChunkJob::Execute()
{
    StorageChunkWriter writer;

    Log_Message("Writing chunk %U to file...", fileChunk->GetChunkID());
    writer.Write(env, fileChunk);

    Callable* c = onComplete;
    delete this;
    IOProcessor::Complete(c);
}

StorageArchiveLogSegmentJob::StorageArchiveLogSegmentJob(StorageEnvironment* env_, 
 StorageLogSegment* logSegment_, const char* script_, Callable* onComplete_)
{
    env = env_;
    logSegment = logSegment_;
    script = script_;
    onComplete = onComplete_;
}

void StorageArchiveLogSegmentJob::Execute()
{
    Buffer  dest;
    Buffer  cmdline;
    
    if (ReadBuffer::Cmp(script, "$archive") == 0)
    {
        Log_Message("Archiving log segment %U...", logSegment->GetLogSegmentID());

        dest.Write(env->archivePath);
        dest.Appendf("log.%020U", logSegment->GetLogSegmentID());
        dest.NullTerminate();

        FS_Rename(logSegment->filename.GetBuffer(), dest.GetBuffer());
    }
    else if (ReadBuffer::Cmp(script, "$delete") == 0)
    {
        Log_Message("Deleting archive log segment %U...", logSegment->GetLogSegmentID());
        FS_Delete(logSegment->filename.GetBuffer());
    }
    else
    {
        Log_Message("Executing script on archive log segment %U (%s)...", 
         logSegment->GetLogSegmentID(), script);
        
        EvalScriptVariables();
        ShellExec(command.GetBuffer());
    }

    Callable* c = onComplete;
    delete this;
    IOProcessor::Complete(c);
}

void StorageArchiveLogSegmentJob::EvalScriptVariables()
{
    Buffer      var;
    const char* p;
    bool        inVar;
    
    p = script;
    inVar = false;
    while (*p)
    {
        // Replace $(variableName) in the script to the value of a config variable
        if (p[0] == '$' && p[1] == '(')
        {
            inVar = true;
            p += 2;         // skip $(
        }
        
        if (inVar && p[0] == ')')
        {
            var.NullTerminate();
            command.Append(GetVarValue(var.GetBuffer()));
            var.Reset();
            inVar = false;
            p += 1;         // skip )
        }

        if (*p == 0)
            break;
        
        if (inVar)
            var.Append(*p);
        else
            command.Append(*p);
        
        p++;
    }
    
    command.NullTerminate();
}

const char* StorageArchiveLogSegmentJob::GetVarValue(const char* var)
{
    if (strcmp(var, "archiveFile") == 0)
        return logSegment->filename.GetBuffer();
    else
        return configFile.GetValue(var, "");
}

StorageDeleteMemoChunkJob::StorageDeleteMemoChunkJob(StorageMemoChunk* chunk_)
{
    chunk = chunk_;
}

void StorageDeleteMemoChunkJob::Execute()
{
    delete chunk;
    delete this;
}

StorageDeleteFileChunkJob::StorageDeleteFileChunkJob(StorageFileChunk* chunk_)
{
    chunk = chunk_;
}

void StorageDeleteFileChunkJob::Execute()
{
    Log_Message("Deleting chunk %U", chunk->GetChunkID());
    
    FS_Delete(chunk->GetFilename().GetBuffer());

    delete chunk;
    delete this;
}

StorageMergeChunkJob::StorageMergeChunkJob(StorageEnvironment* env_,
 ReadBuffer filename1_, ReadBuffer filename2_,
 StorageFileChunk* mergeChunk_, ReadBuffer firstKey_, ReadBuffer lastKey_, Callable* onComplete_)
{
    env = env_;
    filename1.Write(filename1_);
    filename2.Write(filename2_);
    firstKey.Write(firstKey_);
    lastKey.Write(lastKey_);
    mergeChunk = mergeChunk_;
    onComplete = onComplete_;
}

void StorageMergeChunkJob::Execute()
{
    StorageChunkMerger merger;
    
    Log_Message("Merging file %.*s and %.*s into chunk %U...",
     filename1.GetLength()-1, filename1.GetBuffer(),
     filename2.GetLength()-1, filename2.GetBuffer(),
     mergeChunk->GetChunkID());
    merger.Merge(env, filename1, filename2, mergeChunk, firstKey, lastKey);
    Log_Message("Done merging.");

    Callable* c = onComplete;
    delete mergeChunk;
    delete this;
    IOProcessor::Complete(c);
}
