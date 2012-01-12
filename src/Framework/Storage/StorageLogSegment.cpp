#include "StorageLogSegment.h"

StorageLogSegment::StorageLogSegment(uint64_t trackID_, uint64_t logSegmentID_)
{
    prev = next = this;
    trackID = trackID_;
    logSegmentID = logSegmentID_;
    
    filename.Writef("log.%020U.%020U", trackID, logSegmentID);
    filename.NullTerminate();
}

uint64_t StorageLogSegment::GetTrackID()
{
    return trackID;
}

uint64_t StorageLogSegment::GetLogSegmentID()
{
    return logSegmentID;
}

const char* StorageLogSegment::GetFilename()
{
    return filename.GetBuffer();
}

StorageLogSegmentWriter& StorageLogSegment::GetWriter()
{
    return writer;
}
