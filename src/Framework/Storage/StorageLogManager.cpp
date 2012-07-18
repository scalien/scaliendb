#include "StorageLogManager.h"
#include "StorageEnvironment.h"

StorageLogManager::~StorageLogManager()
{
    Track* track;

    FOREACH(track, tracks)
        track->logSegments.DeleteList();
}

StorageLogManager::Track* StorageLogManager::CreateTrack(uint64_t trackID)
{
    Track track;

    track.trackID = trackID;
    tracks.Append(track);

    return tracks.Last();
}

void StorageLogManager::DeleteTrack(uint64_t trackID)
{
    Track* track;
    
    track = GetTrack(trackID);

    track->deleted = true;
}

StorageLogManager::LogSegment* StorageLogManager::CreateLogSegment(uint64_t trackID, uint64_t logSegmentID, ReadBuffer filename)
{
    LogSegment*     logSegment;
    Track* track;

    logSegment = new LogSegment;
    logSegment->trackID = trackID;
    logSegment->logSegmentID = logSegmentID;
    logSegment->filename.Write(filename);

    track = GetTrack(trackID);
    ASSERT(track);

    track->logSegments.Append(logSegment);

    return logSegment;
}

void StorageLogManager::DeleteLogSegment(LogSegment* logSegment)
{
    Track* track;

    track = GetTrack(logSegment->GetTrackID());
    if (!track)
        ASSERT_FAIL();

    track->logSegments.Delete(logSegment);
}

StorageLogManager::Track* StorageLogManager::GetTrack(uint64_t trackID)
{
    Track*      track;

    FOREACH(track, tracks)
    {
        if (track->trackID == trackID)
            return track;
    }

    return NULL;
}

unsigned StorageLogManager::GetNumLogSegments(uint64_t trackID)
{
    Track* track;

    track = GetTrack(trackID);
    if (!track)
        return 0;

    return track->logSegments.GetLength();
}

StorageLogManager::LogSegment* StorageLogManager::GetHead(uint64_t trackID)
{
    uint64_t    logSegmentID;
    Track*      track;
    LogSegment* logSegment;
    Buffer      filename;

    track = GetTrack(trackID);
    if (!track)
        return NULL;

    logSegment = track->logSegments.Last();
    if (logSegment && logSegment->IsOpen())
        return logSegment;

    if (logSegment)
        logSegmentID = logSegment->GetLogSegmentID() + 1;
    else
        logSegmentID = 1;
    filename.Writef("log.%020U.%020U", trackID, logSegmentID);
    logSegment = CreateLogSegment(track->trackID, logSegmentID, filename);
    logSegment->Open(env->logPath, track->trackID, logSegmentID, env->GetConfig().GetSyncGranularity());
    return logSegment;
}

StorageLogManager::LogSegment* StorageLogManager::GetTail(uint64_t trackID)
{
    Track*      track;

    track = GetTrack(trackID);
    if (!track)
        return NULL;

    return track->logSegments.First();
}

uint64_t StorageLogManager::GetMemoryUsage()
{
    uint64_t        totalSize;
    Track*          track;
    LogSegment*     logSegment;

    totalSize = 0;

    FOREACH(track, tracks)
    {
        FOREACH (logSegment, track->logSegments)
            totalSize += logSegment->GetWriteBufferSize();
    }

    return totalSize;
}

uint64_t StorageLogManager::GetDiskUsage()
{
    uint64_t        totalSize;
    Track*          track;
    LogSegment*     logSegment;

    totalSize = 0;

    FOREACH(track, tracks)
    {
        FOREACH (logSegment, track->logSegments)
            totalSize += logSegment->GetOffset();
    }

    return totalSize;
    
}
