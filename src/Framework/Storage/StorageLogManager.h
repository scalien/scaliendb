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
        uint16_t        trackID;
        LogSegmentList  logSegments;
    };

    ~StorageLogManager();

    Track*          CreateTrack(uint64_t trackID);
    LogSegment*     Add(uint64_t trackID, uint64_t logSegmentID, ReadBuffer filename);
    void            Delete(LogSegment* logSegment);

    Track*          GetTrack(uint64_t trackID);
    LogSegment*     GetHead(uint64_t trackID);
    LogSegment*     GetTail(uint64_t trackID);

    uint64_t        GetMemoryUsage();

    StorageEnvironment* env;

    List<Track>     tracks;
};

#endif
