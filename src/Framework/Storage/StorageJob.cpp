#include "StorageJob.h"
#include "System/IO/IOProcessor.h"
#include "System/FileSystem.h"
#include "System/Config.h"
#include "System/Stopwatch.h"
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
    StorageChunkSerializer  serializer;
    Stopwatch               sw;

    Log_Debug("Serializing chunk %U in memory...", memoChunk->GetChunkID());
    sw.Start();
    serializer.Serialize(memoChunk);
    sw.Stop();
    Log_Debug("Done serializing, elapsed: %U", memoChunk->GetChunkID(), (uint64_t) sw.Elapsed());
    
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
    StorageChunkWriter  writer;
    Stopwatch           sw;

    Log_Debug("Writing chunk %U to file...", fileChunk->GetChunkID());
    sw.Start();
    writer.Write(env, fileChunk);
    sw.Stop();
    Log_Debug("Chunk %U written, elapsed: %U", fileChunk->GetChunkID(), (uint64_t) sw.Elapsed());

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
        Log_Debug("Archiving log segment %U...", logSegment->GetLogSegmentID());

        dest.Write(env->archivePath);
        dest.Appendf("log.%020U", logSegment->GetLogSegmentID());
        dest.NullTerminate();

        FS_Rename(logSegment->filename.GetBuffer(), dest.GetBuffer());
    }
    else if (ReadBuffer::Cmp(script, "$delete") == 0)
    {
        Log_Debug("Deleting archive log segment %U...", logSegment->GetLogSegmentID());
        FS_Delete(logSegment->filename.GetBuffer());
    }
    else
    {
        Log_Debug("Executing script on archive log segment %U (%s)...", 
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
            p += 2;         // skip "$("
        }
        
        if (inVar && p[0] == ')')
        {
            var.NullTerminate();
            command.Append(GetVarValue(var.GetBuffer()));
            var.Reset();
            inVar = false;
            p += 1;         // skip ")"
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
    Stopwatch   sw;
    
    Log_Debug("Deleting chunk %U", chunk->GetChunkID());
    sw.Start();
    
    FS_Delete(chunk->GetFilename().GetBuffer());

    delete chunk;
    delete this;

    sw.Stop();
    Log_Debug("Deleted, elapsed: %U", (uint64_t) sw.Elapsed());
}

StorageMergeChunkJob::StorageMergeChunkJob(StorageEnvironment* env_,
 List<Buffer*>& filenames_,
 StorageFileChunk* mergeChunk_, ReadBuffer firstKey_, ReadBuffer lastKey_, Callable* onComplete_)
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
    onComplete = onComplete_;
}

void StorageMergeChunkJob::Execute()
{
    bool                ret;
    StorageChunkMerger  merger;
    Buffer**            itFilename;
    
    Log_Debug("Merging %u chunks into chunk %U...",
     filenames.GetLength(),
     mergeChunk->GetChunkID());
    ret = merger.Merge(env, filenames, mergeChunk, firstKey, lastKey);
    if (ret)
        Log_Debug("Done merging.");

    FOREACH_FIRST (itFilename, filenames)
    {
        delete *itFilename;
        filenames.Remove(itFilename);
    }

    Callable* c = onComplete;
    delete this;
    
    IOProcessor::Complete(c);
}
