#include "StorageLogManager.h"
#include "StorageEnvironment.h"

StorageLogManager::~StorageLogManager()
{
    Track* track;

    FOREACH(track, tracks)
        track->logSegments.DeleteList();
}

void StorageLogManager::SetLogPath(ReadBuffer logPath_)
{
    logPath.Write(logPath_);
    logPath.NullTerminate();
}

StorageLogManager::Track* StorageLogManager::CreateTrack(uint64_t trackID)
{
    Track track;

    track.trackID = trackID;
    tracks.Append(track);

    return tracks.Last();
}

StorageLogManager::LogSegment* StorageLogManager::Add(uint64_t trackID, uint64_t logSegmentID)
{
    LogSegment* logSegment;
    Track*      track;

    logSegment = new LogSegment(trackID, logSegmentID);

    track = GetTrack(trackID);
    ASSERT(track);

    track->logSegments.Append(logSegment);

    return logSegment;
}

void StorageLogManager::Delete(LogSegment* logSegment)
{
    Track* track;

    FOREACH(track, tracks)
    {
        if (track->trackID == logSegment->GetTrackID())
        {
            track->logSegments.Delete(logSegment);
            return;
        }
    }

    ASSERT_FAIL();
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

List<StorageLogManager::Track>& StorageLogManager::GetTracks()
{
    return tracks;
}

StorageLogManager::LogSegment* StorageLogManager::GetHead(uint64_t trackID)
{
    uint64_t    logSegmentID;
    Track*      track;
    LogSegment* logSegment;

    FOREACH(track, tracks)
    {
        if (track->trackID == trackID)
        {
            logSegment = track->logSegments.Last();
            if (logSegment && logSegment->GetWriter().IsOpen())
            {
                return logSegment;
            }
            else
            {
                if (logSegment)
                    logSegmentID = logSegment->GetLogSegmentID() + 1;
                else
                    logSegmentID = 1;
                logSegment = Add(track->trackID, logSegmentID);
                logSegment->GetWriter().Open(StaticPrint("%B/%s", &logPath, logSegment->GetFilename()));
                return logSegment;
            }
        }
    }

    return NULL;
}

StorageLogManager::LogSegment* StorageLogManager::GetTail(uint64_t trackID)
{
    Track*      track;

    FOREACH(track, tracks)
    {
        if (track->trackID == trackID)
            return track->logSegments.First();
    }

    return NULL;
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
            totalSize += logSegment->GetWriter().GetWriteBufferSize();
    }

    return totalSize;
}
