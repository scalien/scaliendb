#ifndef STORAGECHUNKWRITER_H
#define STORAGECHUNKWRITER_H

#include "FDGuard.h"
#include "StorageFileChunk.h"

/*
===============================================================================================

 StorageChunkWriter

===============================================================================================
*/

class StorageChunkWriter
{
public:
    bool                    Write(StorageFileChunk* file);

private:
    bool                    WriteBuffer();

    bool                    WriteEmptyHeaderPage();
    bool                    WriteHeaderPage();
    bool                    WriteDataPages();
    bool                    WriteIndexPage();
    bool                    WriteBloomPage();

    StorageFileChunk*       file;
    FDGuard                 fd;
    Buffer                  writeBuffer;
};

#endif
