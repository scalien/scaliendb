#ifndef STORAGEPAGE_H
#define STORAGEPAGE_H

#include "System/Platform.h"

/*
===============================================================================================

 StoragePage

===============================================================================================
*/

class StoragePage
{
public:    
    void        SetOffset(uint64_t);

private:
    uint64_t    offset;
};

#endif
