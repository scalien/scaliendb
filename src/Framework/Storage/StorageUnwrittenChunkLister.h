#ifndef STORAGEUNWRITTENCHUNKLISTER_H
#define STORAGEUNWRITTENCHUNKLISTER_H

#include "StorageChunkLister.h"
#include "StorageFileChunk.h"

/*
===============================================================================================
 
 StorageUnwrittenChunkLister
 
===============================================================================================
*/

class StorageUnwrittenChunkLister : public StorageChunkLister
{
public:
    StorageUnwrittenChunkLister();
    
    void                    Init(StorageFileChunk* chunk, ReadBuffer& startKey, 
                             unsigned count, unsigned offset);

    void                    Load();
    
    StorageFileKeyValue*    First(ReadBuffer& firstKey);
    StorageFileKeyValue*    Next(StorageFileKeyValue*);
    
    uint64_t                GetNumKeys();

private:
    StorageFileKeyValue*    NextChunkKeyValue(StorageFileChunk* chunk, uint32_t& index, 
                             StorageFileKeyValue* kv);

    StorageDataPage         dataPage;
};

#endif
