#ifndef STORAGELOGMANAGER_H
#define STORAGELOGMANAGER_H

#include "System/Containers/InList.h"
#include "System/Containers/List.h"
#include "StorageLogSegment.h"

/*
===============================================================================================

 StorageLogManager

===============================================================================================
*/

class StorageEnvironment;

class StorageLogManager
{
public:

    typedef StorageLogSegment LogSegment;
    typedef InList<LogSegment> LogSegmentList;

    struct Track
    {
        bool            deleted;
        uint64_t        trackID;
        LogSegmentList  logSegments;

        Track()         { deleted = false; }
    };

    ~StorageLogManager();

    Track*          CreateTrack(uint64_t trackID);
    void            DeleteTrack(uint64_t trackID);

    LogSegment*     CreateLogSegment(uint64_t trackID, uint64_t logSegmentID, ReadBuffer filename);
    void            DeleteLogSegment(LogSegment* logSegment);

    Track*          GetTrack(uint64_t trackID);
    LogSegment*     GetHead(uint64_t trackID);
    LogSegment*     GetTail(uint64_t trackID);

    uint64_t        GetMemoryUsage();

    StorageEnvironment* env;

    List<Track>     tracks;
};

#endif
