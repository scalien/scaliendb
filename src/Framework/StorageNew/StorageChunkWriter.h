#ifndef STORAGECHUNKWRITER_H
#define STORAGECHUNKWRITER_H

#include "System/Buffers/Buffer.h"
#include "FDGuard.h"
//#include "StorageHeaderPage.h"
//#include "StorageDataPage.h"
//#include "StorageIndexPage.h"
//#include "StorageBloomPage.h"

class StorageChunk; // forward

/*
===============================================================================================

 StorageChunkWriter

===============================================================================================
*/

class StorageChunkWriter
{
public:
    bool                    Write(const char* filename, StorageChunk* chunk);

private:
    bool                    WriteBuffer();

    bool                    WriteEmptyHeaderPage();
    bool                    WriteHeader(StorageChunk* chunk);
    bool                    WriteDataPages(StorageChunk* chunk);
    bool                    WriteIndexPage();
    bool                    WriteBloomPage();

    FDGuard                 fd;
    Buffer                  writeBuffer;

    StorageChunkHeaderPage  headerPage;
    StorageChunkDataPage    dataPage;
    StorageChunkIndexPage   indexPage;
    StorageChunkBloomPage   bloomPage;

    uint64_t                offset;
    uint64_t                indexPageOffset;
    uint64_t                bloomPageOffset;
    uint32_t                indexPageSize;
    uint32_t                bloomPageSize;
};

#endif
