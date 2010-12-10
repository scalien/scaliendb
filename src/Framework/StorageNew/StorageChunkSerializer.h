#ifndef STORAGECHUNKSERIALIZER_H
#define STORAGECHUNKSERIALIZER_H

#include "System/Platform.h"

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
