#ifndef STORAGECHUNKMERGER_H
#define STORAGECHUNKMERGER_H

#include "System/Buffers/Buffer.h"
#include "System/Containers/List.h"
#include "FDGuard.h"
#include "StorageChunkReader.h"
#include "StorageHeaderPage.h"
#include "StorageDataPage.h"
#include "StorageIndexPage.h"
#include "StorageBloomPage.h"

class StorageEnvironment;   // forward
class StorageChunk;         // forward

/*
===============================================================================================

 StorageChunkMerger

===============================================================================================
*/

class StorageChunkMerger
{
public:
    bool                    Merge(
                             StorageEnvironment* env,
                             List<Buffer*>& filenames,
                             StorageFileChunk* mergeChunk,
                             ReadBuffer firstKey, ReadBuffer lastKey);
                             // filename1 is older than filename2

    void                    OnMergeFinished();

private:
    bool                    WriteBuffer();
    void                    UnloadChunk();
    
    bool                    WriteEmptyHeaderPage();
    bool                    WriteHeaderPage();
    bool                    WriteDataPages(ReadBuffer firstKey, ReadBuffer lastKey);
    bool                    WriteIndexPage();
    bool                    WriteBloomPage();
    
    bool                    IsDone();
    StorageFileKeyValue*    GetSmallest();
    StorageFileKeyValue*    Next(ReadBuffer& lastKey);
    void                    YieldDiskReads();

    FDGuard                 fd;
    Buffer                  writeBuffer;
    Buffer                  firstKey, lastKey;

    uint64_t                numKeys;
    uint64_t                offset;
    uint64_t                lastSyncOffset;
    uint64_t                lastReadTime;
    unsigned                lastNumReads;

    StorageChunkReader*     readers;
    unsigned                numReaders;
    StorageFileKeyValue**   iterators;

    StorageEnvironment*     env;
    StorageFileChunk*       mergeChunk;

    uint64_t                minLogSegmentID;
    uint64_t                maxLogSegmentID;
    uint64_t                maxLogCommandID;
};

#endif
