#ifndef STORAGECHUNKFILE_H
#define STORAGECHUNKFILE_H

#include "StorageChunk.h"
#include "StorageHeaderPage.h"
#include "StorageIndexPage.h"
#include "StorageBloomPage.h"
#include "StorageDataPage.h"

/*
===============================================================================================

 StorageChunkFile

===============================================================================================
*/

class StorageChunkSerializer;
class StorageChunkWriter;

class StorageChunkFile : public StorageChunk
{
    friend class StorageChunkSerializer;
    friend class StorageChunkWriter;

public:
    StorageChunkFile();
    ~StorageChunkFile();

    void                SetFilename(Buffer& filename);
    Buffer&             GetFilename();

    bool                IsWritten();

    uint64_t            GetChunkID();
    bool                UseBloomFilter();
    
    bool                Get(ReadBuffer& key, ReadBuffer& value);
    
    uint64_t            GetLogSegmentID();
    uint32_t            GetLogCommandID();
    
    uint64_t            GetSize();

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
