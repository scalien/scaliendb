#ifndef STORAGECHUNKMERGER_H
#define STORAGECHUNKMERGER_H

#include "System/Buffers/Buffer.h"
#include "FDGuard.h"
#include "StorageChunkReader.h"
//#include "StorageHeaderPage.h"
//#include "StorageDataPage.h"
//#include "StorageIndexPage.h"
//#include "StorageBloomPage.h"

class StorageChunk; // forward

/*
===============================================================================================

 StorageChunkMerger

===============================================================================================
*/

class StorageChunkMerger
{
public:
    bool                Merge(
                         const char* rFilename1, const char* rFilename2, const char* wFilename
                         uin64_t shardID, uint64_t chunkID, bool useBloomFilter,     
                         Buffer& firstKey, Buffer& lastKey);
                        // file1 is older than file2

private:
    StorageKeyValue*    Merge(StorageKeyValue* it1, StorageKeyValue* it2);
    bool                WriteBuffer();

    bool                WriteEmptyHeaderPage();
    bool                WriteHeaderPage(uint64_t shardID, uint64_t chunkID, bool useBloomFilter);
    bool                WriteDataPages(Buffer& firstKey, Buffer& lastKey);
    bool                WriteIndexPage();
    bool                WriteBloomPage();

    FDGuard             fd;
    Buffer              writeBuffer;

    StorageHeaderPage   headerPage;
    StorageDataPage     dataPage;
    StorageIndexPage    indexPage;
    StorageBloomPage    bloomPage;

    uint64_t            numKeys;
    uint64_t            offset;
    uint64_t            indexPageOffset;
    uint64_t            bloomPageOffset;
    uint32_t            indexPageSize;
    uint32_t            bloomPageSize;

    StorageChunkReader  reader1;
    StorageChunkReader  reader2;

    StorageKeyValue     mergedKeyValue;
};

#endif
