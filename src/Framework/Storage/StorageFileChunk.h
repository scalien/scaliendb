#ifndef STORAGEFILECHUNK_H
#define STORAGEFILECHUNK_H

#include "System/IO/FD.h"
#include "StorageChunk.h"
#include "StorageHeaderPage.h"
#include "StorageIndexPage.h"
#include "StorageBloomPage.h"
#include "StorageDataPage.h"

class StorageAsyncGet;

/*
===============================================================================================

 StorageFileChunk

===============================================================================================
*/

class StorageFileChunk : public StorageChunk
{
public:
    StorageFileChunk();
    ~StorageFileChunk();

    void                ReadHeaderPage();

    void                SetFilename(ReadBuffer filename);
    Buffer&             GetFilename();

    bool                OpenForReading();

    ChunkState          GetChunkState();
    
    void                NextBunch(StorageBulkCursor& cursor, StorageShard* shard);

    uint64_t            GetChunkID();
    bool                UseBloomFilter();
        
    StorageKeyValue*    Get(ReadBuffer& key);
    void                AsyncGet(StorageAsyncGet* asyncGet);
    
    uint64_t            GetMinLogSegmentID();
    uint64_t            GetMaxLogSegmentID();
    uint32_t            GetMaxLogCommandID();
    
    ReadBuffer          GetFirstKey();
    ReadBuffer          GetLastKey();

    uint64_t            GetSize();
    ReadBuffer          GetMidpoint();
    
    void                AddPagesToCache();
    void                RemovePagesFromCache();

    void                OnBloomPageEvicted();
    void                OnIndexPageEvicted();
    void                OnDataPageEvicted(uint32_t index);
    void                LoadBloomPage();
    void                LoadIndexPage();
    void                LoadDataPage(uint32_t index, uint32_t offset, bool bulk = false, bool keysOnly = false);
    StoragePage*        AsyncLoadBloomPage();
    StoragePage*        AsyncLoadIndexPage();
    StoragePage*        AsyncLoadDataPage(uint32_t index, uint32_t offset);

    bool                RangeContains(ReadBuffer key);

    StorageFileChunk*   prev;
    StorageFileChunk*   next;

    void                AppendDataPage(StorageDataPage* dataPage);
    void                AllocateDataPageArray();
    void                ExtendDataPageArray();
    bool                ReadPage(uint32_t offset, Buffer& buffer, bool keysOnly = false);

    bool                written;
    StorageHeaderPage   headerPage;
    StorageIndexPage*   indexPage;
    StorageBloomPage*   bloomPage;
    uint32_t            numDataPages;
    uint32_t            dataPagesSize;
    StorageDataPage**   dataPages;
    uint32_t            fileSize;
    Buffer              filename;
    bool                useCache;
    bool                isBloomPageLoading;
    bool                isIndexPageLoading;
    FD                  fd;
};

#endif
