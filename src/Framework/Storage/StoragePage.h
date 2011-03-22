#ifndef STORAGEPAGE_H
#define STORAGEPAGE_H

#include "System/Common.h"
#include "System/Platform.h"
#include "System/Buffers/Buffer.h"

#define STORAGE_DEFAULT_PAGE_GRAN         (4*KiB)

/*
===============================================================================================

 StoragePage

===============================================================================================
*/

class StoragePage
{
public:
    StoragePage();
    virtual ~StoragePage() {}
    
    void                SetOffset(uint64_t);
    uint64_t            GetOffset();

    bool                IsCached();
    
    virtual uint32_t    GetSize() = 0;
    virtual void        Write(Buffer& writeBuffer) = 0;

    virtual void        Unload() = 0;
    
    StoragePage*        prev;
    StoragePage*        next;

private:
    uint64_t            offset;
};

#endif
