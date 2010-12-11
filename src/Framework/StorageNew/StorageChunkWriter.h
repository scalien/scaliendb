#ifndef STORAGECHUNKWRITER_H
#define STORAGECHUNKWRITER_H

#include "FDGuard.h"
#include "StorageChunkFile.h"

/*
===============================================================================================

 StorageChunkWriter

===============================================================================================
*/

class StorageChunkWriter
{
public:
    bool                    Write(const char* filename, StorageChunkFile* file);

private:
    bool                    WriteBuffer();

    bool                    WriteEmptyHeaderPage();
    bool                    WriteHeaderPage();
    bool                    WriteDataPages();
    bool                    WriteIndexPage();
    bool                    WriteBloomPage();

    StorageChunkFile*       file;
    FDGuard                 fd;
    Buffer                  writeBuffer;
};

#endif
