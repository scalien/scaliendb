#ifndef STORAGEFILECHUNK_H
#define STORAGEFILECHUNK_H

#include "StorageChunk.h"
#include "StorageHeaderPage.h"
#include "StorageIndexPage.h"
#include "StorageBloomPage.h"
#include "StorageDataPage.h"

/*
===============================================================================================

 StorageFileChunk

===============================================================================================
*/

class StorageChunkSerializer;
class StorageChunkWriter;
class StorageRecovery;

class StorageFileChunk : public StorageChunk
{
    friend class StorageChunkSerializer;
    friend class StorageChunkWriter;
    friend class StorageRecovery;

public:
    StorageFileChunk();
    ~StorageFileChunk();

    void                ReadHeaderPage();

    void                SetFilename(Buffer& filename);
    Buffer&             GetFilename();

    ChunkState          GetChunkState();

    uint64_t            GetChunkID();
    bool                UseBloomFilter();
        
    StorageKeyValue*    Get(ReadBuffer& key);
    
    uint64_t            GetLogSegmentID();
    uint32_t            GetLogCommandID();
    
    uint64_t            GetSize();
    
    void                AddPagesToCache();
    void                OnBloomPageEvicted();
    void                OnIndexPageEvicted();
    void                OnDataPageEvicted(uint32_t index);
    void                LoadBloomPage();
    void                LoadIndexPage();
    void                LoadDataPage(uint32_t index, uint32_t offset);

    StorageFileChunk*   prev;
    StorageFileChunk*   next;

private:
    void                AppendDataPage(StorageDataPage* dataPage);
    void                ExtendDataPageArray();
    bool                ReadPage(uint32_t offset, Buffer& buffer);

    bool                written;
    StorageHeaderPage   headerPage;
    StorageIndexPage*   indexPage;
    StorageBloomPage*   bloomPage;
    uint32_t            numDataPages;
    uint32_t            dataPagesSize;
    StorageDataPage**   dataPages;
    uint32_t            fileSize;
    Buffer              filename;
};

#endif
