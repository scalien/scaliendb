#ifndef STORAGECHUNKREADER_H
#define STORAGECHUNKREADER_H

#include "System/Buffers/Buffer.h"
#include "FDGuard.h"

/*
===============================================================================================

 StorageChunkReader

===============================================================================================
*/

class StorageChunkReader
{
public:
    bool                Open(const char* filename);
    void                Close();

    bool                ReadHeaderPage();
    uint64_t            GetLogSegmentID();
    uint64_t            GetNumKeys();

    StorageKeyValue*    First();
    StorageKeyValue*    Next(StorageKeyValue* it);

private:
    uint64_t            offset;
    FDGuard             fd;
    Buffer              readBuffer;
    StorageHeaderPage   headerPage;
    StorageDataPage     dataPage;
};

#endif
