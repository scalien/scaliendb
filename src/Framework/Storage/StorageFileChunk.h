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

    void                Init();
    void                Close();

    void                ReadHeaderPage();

    void                SetFilename(ReadBuffer filename);
    void                SetFilename(Buffer& chunkPath, uint64_t chunkID);
    Buffer&             GetFilename();

    bool                OpenForReading();

    ChunkState          GetChunkState();
    
    void                NextBunch(StorageBulkCursor& cursor, StorageShard* shard);

    uint64_t            GetChunkID();
    bool                UseBloomFilter();
    bool                IsMerged();
        
    StorageKeyValue*    Get(ReadBuffer& key);
    void                AsyncGet(StorageAsyncGet* asyncGet);
    
    uint64_t            GetMinLogSegmentID();
    uint64_t            GetMaxLogSegmentID();
    uint32_t            GetMaxLogCommandID();
    
    ReadBuffer          GetFirstKey();
    ReadBuffer          GetLastKey();

    uint64_t            GetSize();
    uint64_t            GetPartialSize(ReadBuffer firstKey, ReadBuffer lastKey);
    ReadBuffer          GetMidpoint();

    bool                IsEmpty();
    
    void                AddPagesToCache();
    void                RemovePagesFromCache();

    void                OnBloomPageEvicted();
    void                OnIndexPageEvicted();
    void                OnDataPageEvicted(uint32_t index);
    void                LoadBloomPage();
    void                LoadIndexPage();
    void                LoadDataPage(uint32_t index, uint64_t offset, bool bulk = false, bool keysOnly = false, StorageDataPage* dataPage = NULL);
    StoragePage*        AsyncLoadBloomPage();
    StoragePage*        AsyncLoadIndexPage();
    StoragePage*        AsyncLoadDataPage(uint32_t index, uint64_t offset);

    void                SetBloomPage(StorageBloomPage* bloomPage);
    void                SetIndexPage(StorageIndexPage* indexPage);
    void                SetDataPage(StorageDataPage* dataPage);

    bool                RangeContains(ReadBuffer key);

    void                AppendDataPage(StorageDataPage* dataPage);
    void                AllocateDataPageArray();

    StorageFileChunk*   prev;
    StorageFileChunk*   next;

    // TODO: change these to private
    bool                written;
    bool                useCache;
    bool                writeError;
    StorageHeaderPage   headerPage;
    StorageIndexPage*   indexPage;
    StorageBloomPage*   bloomPage;
    uint32_t            numDataPages;
    uint32_t            dataPagesSize;
    StorageDataPage**   dataPages;
    uint64_t            fileSize;

private:
    void                ExtendDataPageArray();
    bool                ReadPage(uint64_t offset, Buffer& buffer, bool keysOnly = false);

    Buffer              filename;
    FD                  fd;
};

#endif
