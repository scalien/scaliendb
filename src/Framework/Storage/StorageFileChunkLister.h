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
    void                    Init(ReadBuffer filename, bool keysOnly);

    void                    Load();
    
    StorageFileKeyValue*    First(ReadBuffer& firstKey);
    StorageFileKeyValue*    Next(StorageFileKeyValue*);

    uint64_t                GetNumKeys();

private:
    Buffer                  filename;
    bool                    keysOnly;
    StorageChunkReader      reader;
};

#endif
