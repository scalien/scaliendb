#ifndef STORAGECHUNKSERIALIZER_H
#define STORAGECHUNKSERIALIZER_H

#include "System/Platform.h"

class StorageChunkMemory; // forward
class StorageFileChunk;  // forward

/*
===============================================================================================

 StorageChunkSerializer

===============================================================================================
*/

class StorageChunkSerializer
{
public:
    StorageFileChunk*       Serialize(StorageChunkMemory* memoChunk);

private:
    bool                    WriteHeaderPage();
    bool                    WriteDataPages();
    bool                    WriteIndexPage();
    bool                    WriteBloomPage();

    StorageChunkMemory*     memoChunk;
    StorageFileChunk*       fileChunk;
    uint64_t                offset;
};

#endif
