#ifndef STORAGEINDEXPAGE_H
#define STORAGEINDEXPAGE_H

#include "System/Platform.h"
#include "System/Common.h"
#include "System/Buffers/ReadBuffer.h"
#include "System/Containers/SortedList.h"
#include "System/Containers/InSortedList.h"
#include "System/Containers/InTreeMap.h"
#include "StoragePage.h"
#include "StorageKeyIndex.h"

#define INDEXPAGE_FIX_OVERHEAD      16
#define INDEXPAGE_KV_OVERHEAD       8
#define INDEXPAGE_HEADER_SIZE       12

/*
===============================================================================

 StorageIndexPage

===============================================================================
*/

class StorageIndexPage : public StoragePage
{
    typedef InTreeMap<StorageKeyIndex> KeyIndexMap;

public:
    StorageIndexPage();
    ~StorageIndexPage();
    
    void                    SetPageSize(uint32_t pageSize);
    void                    SetNumDataPageSlots(uint32_t numDataPageSlots);
    
    void                    Add(ReadBuffer key, uint32_t index, bool copy = true);
    void                    Update(ReadBuffer key, uint32_t index, bool copy = true);
    void                    Remove(ReadBuffer key);

    bool                    IsEmpty();
    ReadBuffer              FirstKey();
    uint32_t                NumEntries();
    int32_t                 GetMaxDataPageIndex();
    int32_t                 Locate(ReadBuffer& key, Buffer* nextKey = NULL);
    uint32_t                NextFreeDataPage();
    bool                    IsOverflowing();
    bool                    HasKey(const ReadBuffer& key);

    void                    DumpKeys();

    void                    Read(ReadBuffer& buffer);
    bool                    CheckWrite(Buffer& buffer);
    bool                    Write(Buffer& buffer);
    
private:
    void                    UpdateHighestIndex(uint32_t index);

    uint32_t                numDataPageSlots;
    uint32_t                required;
    int32_t                 maxDataPageIndex;
    KeyIndexMap             keys;
    SortedList<uint32_t>    freeDataPages;
    
    friend class StorageFile;
};


#endif
