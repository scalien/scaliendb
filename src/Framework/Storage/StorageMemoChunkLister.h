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
    
    void                    Init(StorageMemoChunk* chunk, ReadBuffer& startKey, unsigned count, 
                             bool keysOnly);

    void                    Load();
    
    StorageFileKeyValue*    First(ReadBuffer& firstKey);
    StorageFileKeyValue*    Next(StorageFileKeyValue*);
    
    uint64_t                GetNumKeys();
    StorageDataPage*        GetDataPage();

private:
    StorageDataPage         dataPage;
};

#endif
