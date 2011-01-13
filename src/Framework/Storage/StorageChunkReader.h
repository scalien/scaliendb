#ifndef STORAGECHUNKREADER_H
#define STORAGECHUNKREADER_H

#include "StorageFileChunk.h"

class StorageChunkReader
{
public:
    void                    Open(ReadBuffer filename);

    StorageFileKeyValue*    First();
    StorageFileKeyValue*    Next(StorageFileKeyValue*);

    uint64_t                GetNumKeys();
    uint64_t                GetMinLogSegmentID();
    uint64_t                GetMaxLogSegmentID();
    uint64_t                GetMaxLogCommandID();

private:
    StorageFileChunk        fileChunk;
    uint32_t                index;
    uint64_t                offset;

};

#endif
