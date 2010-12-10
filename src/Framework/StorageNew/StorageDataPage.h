#ifndef STORAGEDATAPAGE_H
#define STORAGEDATAPAGE_H

#include "StoragePage.h"

/*
===============================================================================================

 StorageDataPage

===============================================================================================
*/

class StorageDataPage : public StoragePage
{
public:
    uint32_t        GetSize();
};

#endif
