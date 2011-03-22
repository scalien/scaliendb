#ifndef STORAGECHUNKREADER_H
#define STORAGECHUNKREADER_H

#include "StorageFileChunk.h"

class StorageChunkReader
{
public:
    void                    Open(ReadBuffer filename, uint64_t preloadThreshold, bool keysOnly = false);

    StorageFileKeyValue*    First(ReadBuffer& firstKey);
    StorageFileKeyValue*    Next(StorageFileKeyValue*);

    uint64_t                GetNumKeys();
    uint64_t                GetMinLogSegmentID();
    uint64_t                GetMaxLogSegmentID();
    uint64_t                GetMaxLogCommandID();

private:
    void                    PreloadDataPages();

    StorageFileChunk        fileChunk;
    uint64_t                offset;
    uint32_t                index;
    uint32_t                prevIndex;
    uint32_t                preloadIndex;
    uint64_t                preloadThreshold;
    bool                    keysOnly;
};

#endif
