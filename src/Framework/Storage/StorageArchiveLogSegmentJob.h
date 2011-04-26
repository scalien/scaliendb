#ifndef STORAGEARCHIVELOGSEGMENTJOB_H
#define STORAGEARCHIVELOGSEGMENTJOB_H

#include "System/Threading/Job.h"
#include "System/Buffers/Buffer.h"

class StorageEnvironment;
class StorageLogSegment;

/*
===============================================================================================

 StorageArchiveLogSegmentJob

===============================================================================================
*/

class StorageArchiveLogSegmentJob : public Job
{
public:
    StorageArchiveLogSegmentJob(StorageEnvironment* env,
     StorageLogSegment* logSegment, const char* script);
    
    void                Execute();
    void                OnComplete();
    
private:
    void                EvalScriptVariables();
    const char*         GetVarValue(const char* var);

    StorageEnvironment* env;
    StorageLogSegment*  logSegment;
    const char*         script;
    Buffer              command;
};

#endif
