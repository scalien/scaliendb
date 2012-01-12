#ifndef STORAGELOGSEGMENT_H
#define STORAGELOGSEGMENT_H

#include "System/Platform.h"
#include "System/Buffers/Buffer.h"
#include "StorageLogSegmentWriter.h"

/*
===============================================================================================

 StorageLogSegment

===============================================================================================
*/

class StorageLogSegment
{
    typedef StorageLogSegmentWriter Writer;
    typedef StorageLogSegment LogSegment;
    
public:
    StorageLogSegment(uint64_t trackID, uint64_t logSegmentID);
    
    uint64_t    GetTrackID();
    uint64_t    GetLogSegmentID();
    const char* GetFilename();
    Writer&     GetWriter();
    
    LogSegment* prev;
    LogSegment* next;

private:
    uint64_t    trackID;
    uint64_t    logSegmentID;
    Buffer      filename;
    Writer      writer;
};

#endif
