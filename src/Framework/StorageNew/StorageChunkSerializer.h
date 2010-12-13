#ifndef STORAGECHUNKSERIALIZER_H
#define STORAGECHUNKSERIALIZER_H

#include "System/Platform.h"

class StorageMemoChunk; // forward
class StorageFileChunk;  // forward

/*
===============================================================================================

 StorageChunkSerializer

===============================================================================================
*/

class StorageChunkSerializer
{
public:
    bool                    Serialize(StorageMemoChunk* memoChunk);

private:
    bool                    WriteHeaderPage();
    bool                    WriteDataPages();
    bool                    WriteIndexPage();
    bool                    WriteBloomPage();

    StorageMemoChunk*       memoChunk;
    StorageFileChunk*       fileChunk;
    uint64_t                offset;
};

#endif
