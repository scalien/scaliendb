#ifndef STORAGECHUNKREADER_H
#define STORAGECHUNKREADER_H

#include "System/Threading/Signal.h"
#include "StorageFileChunk.h"

class StorageChunkReader
{
public:
    void                    Open(ReadBuffer filename, uint64_t preloadThreshold,
                             bool keysOnly = false, bool forwardDirection = true);

    void                    OpenWithFileChunk(StorageFileChunk* fileChunk, uint64_t preloadThreshold,
                             bool keysOnly = false, bool forwardDirection = true);

    void                    SetEndKey(ReadBuffer endKey);
    void                    SetPrefix(ReadBuffer prefix);
    void                    SetCount(unsigned count);

    StorageFileKeyValue*    First(ReadBuffer& firstKey);
    StorageFileKeyValue*    Next(StorageFileKeyValue*);

    StorageDataPage*        FirstDataPage();
    StorageDataPage*        NextDataPage();

    uint64_t                GetNumKeys();
    uint64_t                GetMinLogSegmentID();
    uint64_t                GetMaxLogSegmentID();
    uint64_t                GetMaxLogCommandID();

private:
    void                    PreloadDataPages();
    bool                    LocateIndexAndOffset(StorageIndexPage* indexPage, uint32_t numDataPages, ReadBuffer& firstKey);
    void                    OnLocateIndexAndOffset();

    bool                    keysOnly;
    bool                    forwardDirection;
    uint64_t                offset;
    uint32_t                index;
    uint32_t                prevIndex;
    uint32_t                preloadIndex;
    uint64_t                preloadThreshold;
    StorageFileChunk        fileChunk;
    ReadBuffer              firstKey;
    ReadBuffer              endKey;
    ReadBuffer              prefix;
    unsigned                count;
    unsigned                numRead;
    Signal                  signal;
    StorageIndexPage*       indexPage;
    uint32_t                numDataPages;
};

#endif
