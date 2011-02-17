#ifndef STORAGEFILECHUNKLISTER_H
#define STORAGEFILECHUNKLISTER_H

#include "StorageChunkLister.h"
#include "StorageChunkReader.h"

/*
===============================================================================================
 
StorageFileChunkLister
 
===============================================================================================
*/

class StorageFileChunkLister : public StorageChunkLister
{
public:
    void                    Load(ReadBuffer filename);
    
    StorageFileKeyValue*    First(ReadBuffer& firstKey);
    StorageFileKeyValue*    Next(StorageFileKeyValue*);

    uint64_t                GetNumKeys();

private:
    StorageChunkReader      reader;
};

#endif
