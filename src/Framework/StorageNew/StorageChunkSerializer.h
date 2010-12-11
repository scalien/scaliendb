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
    bool                    WriteHeaderPage();
    bool                    WriteDataPages();
    bool                    WriteIndexPage();
    bool                    WriteBloomPage();

    StorageChunk*           chunk;
    StorageChunkFile*       file;
    uint64_t                offset;
};

#endif
