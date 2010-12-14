#ifndef STORAGEPAGE_H
#define STORAGEPAGE_H

#include "System/Platform.h"
#include "System/Buffers/Buffer.h"

/*
===============================================================================================

 StoragePage

===============================================================================================
*/

class StoragePage
{
public:
    StoragePage();
    
    void                SetOffset(uint64_t);
    uint32_t            GetOffset();
    
    virtual uint32_t    GetSize() = 0;

    virtual void        Write(Buffer& writeBuffer) = 0;
    
    virtual bool        IsLoaded() = 0;
    virtual void        Unload() = 0;
    
    StoragePage*        prev;
    StoragePage*        next;

private:
    uint64_t    offset;
};

#endif
