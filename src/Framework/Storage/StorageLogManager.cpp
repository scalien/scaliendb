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

StorageLogManager::LogSegment* StorageLogManager::Add(uint64_t trackID, uint64_t logSegmentID, ReadBuffer filename)
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

StorageLogManager::LogSegment* StorageLogManager::GetHead(uint64_t trackID)
{
    uint64_t    logSegmentID;
    Track*      track;
    LogSegment* logSegment;
    Buffer      filename;

    FOREACH(track, tracks)
    {
        if (track->trackID == trackID)
        {
            logSegment = track->logSegments.Last();
            if (logSegment && logSegment->IsOpen())
            {
                return logSegment;
            }
            else
            {
                if (logSegment)
                    logSegmentID = logSegment->GetLogSegmentID() + 1;
                else
                    logSegmentID = 1;
                filename.Writef("log.%020U.%020U", trackID, logSegmentID);
                logSegment = Add(track->trackID, logSegmentID, filename);
                logSegment->Open(env->logPath, track->trackID, logSegmentID, env->config.syncGranularity);
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
            totalSize += logSegment->GetWriteBufferSize();
    }

    return totalSize;
}
