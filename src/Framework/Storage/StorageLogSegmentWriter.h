#ifndef STORAGELOGSEGMENTWRITER_H
#define STORAGELOGSEGMENTWRITER_H

#include "System/FileSystem.h"
#include "System/Buffers/Buffer.h"
#include "StorageLogSectionSerializer.h"

/*
===============================================================================================

 StorageLogSegmentWriter

===============================================================================================
*/

class StorageLogSegmentWriter
{
    typedef StorageLogSectionSerializer Serializer;
    
public:
    void        Open(const char* filepath);
    void        Close();

    bool        IsOpen();
    bool        HasUncommitted();
    uint32_t    GetLogCommandID();
    uint32_t    GetCommittedLogCommandID();
    uint64_t    GetFilesize();
    uint64_t    GetWriteBufferSize();

    uint32_t    WriteSet(uint16_t contextID, uint64_t shardID, ReadBuffer key, ReadBuffer value);
    uint32_t    WriteDelete(uint16_t contextID, uint64_t shardID, ReadBuffer key);

    void        Commit();

private:
    void        OpenFile(const char* filepath);
    void        WriteHeader();
    void        WriteSection();
    
    uint32_t    logCommandID;
    uint32_t    committedLogCommandID;
    uint64_t    filesize;
    Buffer      filepathBuf;
    FD          fd;
    Serializer  serializer;
};

#endif
