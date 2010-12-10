#ifndef STORAGECHUNKSERIALIZER_H
#define STORAGECHUNKSERIALIZER_H

#include "System/Platform.h"

#define STORAGE_HEADER_PAGE_SIZE               4096
#define STORAGE_DEFAULT_DATA_PAGE_SIZE         (64*1024)
#define STORAGE_HEADER_PAGE_VERSION            1

class StorageChunk; // forward
class StorageChunkFile;  // forward

/*
===============================================================================================

 StorageChunkSerializer

===============================================================================================
*/

class StorageChunkSerializer
{
public:
    StorageChunkFile*       Serialize(StorageChunk* chunk);

private:
    bool                    WriteHeaderPage(StorageChunk* chunk, StorageChunkFile* file);
    bool                    WriteDataPages(StorageChunk* chunk, StorageChunkFile* file);
    bool                    WriteIndexPage(StorageChunkFile* file);
    bool                    WriteBloomPage(StorageChunkFile* file);

    uint64_t                offset;
};

#endif
