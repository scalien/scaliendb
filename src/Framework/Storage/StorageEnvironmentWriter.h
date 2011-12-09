#ifndef STORAGEENVIRONMENTWRITER_H
#define STORAGEENVIRONMENTWRITER_H

#include "System/Buffers/Buffer.h"
#include "FDGuard.h"

class StorageEnvironment;
class StorageShard;

/*
===============================================================================================

 StorageEnvironment

===============================================================================================
*/

class StorageEnvironmentWriter
{
public:

    bool                    Write(StorageEnvironment* env);
    uint64_t                WriteUnique(StorageEnvironment* env);

private:
    void                    WriteBuffer();
    void                    WriteShard(StorageShard* shard);
    
    StorageEnvironment*     env;
    FDGuard                 fd;
    Buffer                  writeBuffer;
};

#endif
