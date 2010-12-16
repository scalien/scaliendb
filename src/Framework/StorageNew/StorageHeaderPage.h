#ifndef STORAGEHEADERPAGE_H
#define STORAGEHEADERPAGE_H

#include "System/Buffers/Buffer.h"
#include "StoragePage.h"

#define STORAGE_HEADER_PAGE_VERSION     1
#define STORAGE_HEADER_PAGE_SIZE        STORAGE_DEFAULT_PAGE_GRAN

class StorageFileChunk;

/*
===============================================================================================

 StorageHeaderPage

===============================================================================================
*/

class StorageHeaderPage : public StoragePage
{
public:
    StorageHeaderPage(StorageFileChunk* owner);

    uint32_t            GetSize();

    uint64_t            GetChunkID();
    uint64_t            GetLogSegmentID();
    uint32_t            GetLogCommandID();
    uint64_t            GetIndexPageOffset();
    uint32_t            GetIndexPageSize();
    uint64_t            GetBloomPageOffset();
    uint32_t            GetBloomPageSize();

    void                SetChunkID(uint64_t chunkID);
    void                SetLogSegmentID(uint64_t logSegmentID);
    void                SetLogCommandID(uint32_t logCommandID);
    void                SetNumKeys(uint64_t numKeys);
    void                SetUseBloomFilter(bool useBloomFilter);
    void                SetIndexPageOffset(uint64_t indexPageOffset);
    void                SetIndexPageSize(uint32_t indexPageSize);
    void                SetBloomPageOffset(uint64_t bloomPageOffset);
    void                SetBloomPageSize(uint32_t bloomPageSize);

    bool                UseBloomFilter();

    void                Write(Buffer& writeBuffer);

    void                Unload();

private:
    uint64_t            chunkID;
    uint64_t            logSegmentID;
    uint32_t            logCommandID;
    uint64_t            numKeys;
    bool                useBloomFilter;
    uint64_t            indexPageOffset;
    uint32_t            indexPageSize;
    uint64_t            bloomPageOffset;
    uint32_t            bloomPageSize;
    StorageFileChunk*   owner;
};

#endif
