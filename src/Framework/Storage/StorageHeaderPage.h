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
    StorageHeaderPage();

    uint32_t            GetSize();
    uint32_t            GetMemorySize();

    uint64_t            GetChunkID();
    uint64_t            GetMinLogSegmentID();
    uint64_t            GetMaxLogSegmentID();
    uint32_t            GetMaxLogCommandID();
    uint64_t            GetNumKeys();
    uint64_t            GetIndexPageOffset();
    uint32_t            GetIndexPageSize();
    uint64_t            GetBloomPageOffset();
    uint32_t            GetBloomPageSize();
    ReadBuffer          GetFirstKey();
    ReadBuffer          GetLastKey();
    ReadBuffer          GetMidpoint();
    bool                IsMerged();

    void                SetChunkID(uint64_t chunkID);
    void                SetMinLogSegmentID(uint64_t logSegmentID);
    void                SetMaxLogSegmentID(uint64_t logSegmentID);
    void                SetMaxLogCommandID(uint32_t logCommandID);
    void                SetNumKeys(uint64_t numKeys);
    void                SetUseBloomFilter(bool useBloomFilter);
    void                SetIndexPageOffset(uint64_t indexPageOffset);
    void                SetIndexPageSize(uint32_t indexPageSize);
    void                SetBloomPageOffset(uint64_t bloomPageOffset);
    void                SetBloomPageSize(uint32_t bloomPageSize);
    void                SetFirstKey(ReadBuffer firstKey);
    void                SetLastKey(ReadBuffer lastKey);
    void                SetMidpoint(ReadBuffer midPoint);
    void                SetMerged(bool merged);

    bool                UseBloomFilter();

    bool                Read(Buffer& buffer);
    void                Write(Buffer& writeBuffer);

    void                Unload();

private:
    uint64_t            chunkID;
    uint64_t            minLogSegmentID;
    uint64_t            maxLogSegmentID;
    uint32_t            maxLogCommandID;
    uint64_t            numKeys;
    bool                useBloomFilter;
    uint64_t            indexPageOffset;
    uint32_t            indexPageSize;
    uint64_t            bloomPageOffset;
    uint32_t            bloomPageSize;
    Buffer              firstKey;
    Buffer              lastKey;
    Buffer              midpoint;
    bool                merged;
};

#endif
