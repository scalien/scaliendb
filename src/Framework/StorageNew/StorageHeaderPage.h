#ifndef STORAGEHEADERPAGE_H
#define STORAGEHEADERPAGE_H

#include "System/Buffers/Buffer.h"
#include "StoragePage.h"

#define STORAGE_HEADER_PAGE_VERSION     1
#define STORAGE_HEADER_PAGE_SIZE        4096

/*
===============================================================================================

 StorageHeaderPage

===============================================================================================
*/

class StorageHeaderPage : public StoragePage
{
public:
    uint32_t    GetSize();

    void        SetChunkID(uint64_t chunkID);
    void        SetLogSegmentID(uint64_t logSegmentID);
    void        SetLogCommandID(uint64_t logCommandID);
    void        SetNumKeys(uint64_t numKeys);

    void        SetUseBloomFilter(bool useBloomFilter);
    void        SetIndexPageOffset(uint64_t indexOffset);
    void        SetIndexPageSize(uint32_t indexSize);
    void        SetBloomPageOffset(uint64_t bloomOffset);
    void        SetBloomPageSize(uint32_t bloomSize);

private:
    uint64_t    chunkID;
    uint64_t    logSegmentID;
    uint64_t    logCommandID;
    uint64_t    numKeys;
    bool        useBloomFilter;
    uint64_t    indexPageOffset;
    uint64_t    indexPageSize;
    uint64_t    bloomPageOffset;
    uint64_t    bloomPageSize;
};

#endif
