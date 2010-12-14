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

class StorageFileChunk : public StorageChunk
{
    friend class StorageChunkSerializer;
    friend class StorageChunkWriter;

public:
    StorageFileChunk();
    ~StorageFileChunk();

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

    StorageFileChunk*   prev;
    StorageFileChunk*   next;

private:
    void                AppendDataPage(StorageDataPage* dataPage);
    void                ExtendDataPageArray();

    bool                written;
    StorageHeaderPage   headerPage;
    StorageIndexPage    indexPage;
    StorageBloomPage    bloomPage;
    uint32_t            numDataPages;
    uint32_t            dataPagesSize;
    StorageDataPage**   dataPages;
    uint32_t            fileSize;
    Buffer              filename;
};

#endif
