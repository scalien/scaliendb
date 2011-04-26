#include "StorageArchiveLogSegmentJob.h"
#include "System/FileSystem.h"
#include "System/Config.h"
#include "System/Stopwatch.h"
#include "StorageEnvironment.h"

StorageArchiveLogSegmentJob::StorageArchiveLogSegmentJob(StorageEnvironment* env_, 
 StorageLogSegment* logSegment_, const char* script_)
{
    env = env_;
    logSegment = logSegment_;
    script = script_;
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

void StorageArchiveLogSegmentJob::OnComplete()
{
    env->OnLogArchive(this); // deletes this
}
