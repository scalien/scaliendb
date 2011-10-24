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
    void                    Init(ReadBuffer filename, ReadBuffer firstKey, ReadBuffer endKey, ReadBuffer prefix,
                             unsigned count, bool keysOnly, uint64_t preloadBufferSize, bool forwardDirection);

    void                    Load();
    
    void                    SetDirection(bool forwardDirection);
    StorageFileKeyValue*    First(ReadBuffer& firstKey);
    StorageFileKeyValue*    Next(StorageFileKeyValue*);

    uint64_t                GetNumKeys();

private:
    Buffer                  filename;
    bool                    forwardDirection;
    bool                    keysOnly;
    uint64_t                preloadBufferSize;
    StorageChunkReader      reader;
    ReadBuffer              firstKey;
    ReadBuffer              endKey;
    ReadBuffer              prefix;
    unsigned                count;
};

#endif
