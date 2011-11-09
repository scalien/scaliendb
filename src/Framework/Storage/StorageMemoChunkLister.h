#ifndef STORAGEMEMOCHUNKLISTER_H
#define STORAGEMEMOCHUNKLISTER_H

#include "StorageChunkLister.h"
#include "StorageDataPage.h"
#include "StorageMemoChunk.h"

/*
===============================================================================================
 
StorageMemoChunkLister
 
===============================================================================================
*/

class StorageMemoChunkLister : public StorageChunkLister
{
public:
    StorageMemoChunkLister();
    
    void                    Init(StorageMemoChunk* chunk, ReadBuffer& firstKey, ReadBuffer& endKey, ReadBuffer& prefix,
                             unsigned count, bool keysOnly, bool forwardDirection);

    void                    SetDirection(bool forwardDirection);
    StorageFileKeyValue*    First(ReadBuffer& firstKey);
    StorageFileKeyValue*    Next(StorageFileKeyValue*);
    
    uint64_t                GetNumKeys();
    StorageDataPage*        GetDataPage();

private:
    StorageMemoKeyValue*    GetFirstKey(StorageMemoChunk* chunk, 
                             ReadBuffer& firstKey, bool forwardDirection);

    bool                    forwardDirection;
    StorageDataPage         dataPage;
};

#endif
