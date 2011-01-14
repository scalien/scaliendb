#ifndef STORAGECHUNKMERGER_H
#define STORAGECHUNKMERGER_H

#include "System/Buffers/Buffer.h"
#include "FDGuard.h"
#include "StorageChunkReader.h"
#include "StorageHeaderPage.h"
#include "StorageDataPage.h"
#include "StorageIndexPage.h"
#include "StorageBloomPage.h"

class StorageEnvironment;   // forward
class StorageChunk;         // forward

/*
===============================================================================================

 StorageChunkMerger

===============================================================================================
*/

class StorageChunkMerger
{
public:
    bool                    Merge(
                             StorageEnvironment* env,
                             ReadBuffer filename1, ReadBuffer filename2,
                             StorageFileChunk* mergeChunk,
                             ReadBuffer firstKey, ReadBuffer lastKey);
                             // filename1 is older than filename2

private:
    StorageFileKeyValue*    Merge(StorageFileKeyValue* , StorageFileKeyValue* it2);
    bool                    WriteBuffer();
    void                    UnloadChunk();
    
    bool                    WriteEmptyHeaderPage();
    bool                    WriteHeaderPage();
    bool                    WriteDataPages(ReadBuffer firstKey, ReadBuffer lastKey);
    bool                    WriteIndexPage();
    bool                    WriteBloomPage();

    FDGuard                 fd;
    Buffer                  writeBuffer;
    Buffer                  firstKey, lastKey;

    uint64_t                numKeys;
    uint32_t                offset;

    StorageChunkReader      reader1;
    StorageChunkReader      reader2;

    StorageEnvironment*     env;
    StorageFileChunk*       mergeChunk;
};

#endif
