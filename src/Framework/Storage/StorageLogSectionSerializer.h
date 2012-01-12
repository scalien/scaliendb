#ifndef STORAGELOGSECTIONSERIALIZER_H
#define STORAGELOGSECTIONSERIALIZER_H

#include "System/Buffers/Buffer.h"

/*
===============================================================================================

 StorageLogSectionSerializer

===============================================================================================
*/

class StorageLogSectionSerializer
{
public:

    StorageLogSectionSerializer();

    void        Init();
    Buffer&     GetBuffer();
    
    void        SerializeSet(uint16_t contextID, uint64_t shardID, ReadBuffer key, ReadBuffer value);
    void        SerializeDelete(uint16_t contextID, uint64_t shardID, ReadBuffer key);
    void        Finalize();
    
private:
    void        SerializePrefix(char command, uint16_t contextID, uint64_t shardID);
    void        Remember(uint16_t contextID, uint64_t shardID);
    
    Buffer      buffer;
    uint16_t    prevContextID;
    uint64_t    prevShardID;
};

#endif
