#ifndef STORAGECHUNKWRITER_H
#define STORAGECHUNKWRITER_H

#include "System/Buffers/Buffer.h"
#include "FDGuard.h"
//#include "StorageHeaderPage.h"
//#include "StorageDataPage.h"
//#include "StorageIndexPage.h"
//#include "StorageBloomPage.h"

#default STORAGE_DEFAULT_DATA_PAGE_SIZE         (64*1024)

class StorageChunk; // forward

/*
===============================================================================================

 StorageChunkWriter

===============================================================================================
*/

class StorageChunkWriter
{
public:
    bool                Write(const char* filename, StorageChunk* chunk);

private:
    bool                WriteBuffer();

    bool                WriteEmptyHeaderPage();
    bool                WriteHeader(StorageChunk* chunk);
    bool                WriteDataPages(StorageChunk* chunk);
    bool                WriteIndexPage();
    bool                WriteBloomPage();

    FDGuard             fd;
    Buffer              writeBuffer;

    StorageHeaderPage   headerPage;
    StorageDataPage     dataPage;
    StorageIndexPage    indexPage;
    StorageBloomPage    bloomPage;

    uint64_t            offset;
    uint64_t            indexPageOffset;
    uint64_t            bloomPageOffset;
    uint32_t            indexPageSize;
    uint32_t            bloomPageSize;
};

#endif
