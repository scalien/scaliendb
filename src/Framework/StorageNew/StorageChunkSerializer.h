#ifndef STORAGECHUNKSERIALIZER_H
#define STORAGECHUNKSERIALIZER_H

#include "System/Platform.h"

class StorageChunkMemory; // forward
class StorageChunkFile;  // forward

/*
===============================================================================================

 StorageChunkSerializer

===============================================================================================
*/

class StorageChunkSerializer
{
public:
    StorageChunkFile*       Serialize(StorageChunkMemory* memoChunk);

private:
    bool                    WriteHeaderPage();
    bool                    WriteDataPages();
    bool                    WriteIndexPage();
    bool                    WriteBloomPage();

    StorageChunkMemory*     memoChunk;
    StorageChunkFile*       fileChunk;
    uint64_t                offset;
};

#endif
