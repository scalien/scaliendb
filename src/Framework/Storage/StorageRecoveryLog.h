#ifndef STORAGERECOVERYLOG_H
#define STORAGERECOVERYLOG_H

#include "System/IO/FD.h"
#include "System/Platform.h"
#include "System/Buffers/Buffer.h"

class StorageRecoveryLog
{
public:
    bool        Open(const char* filename);
    bool        Close();
    int64_t     GetFileSize();

    bool        WriteOpHeader(uint32_t op, uint64_t total);

    bool        WriteLittle32(uint32_t x);
    bool        WriteLittle64(uint64_t x);
    bool        WriteData(ReadBuffer& data);
    bool        WriteUnbufferedData(ReadBuffer& data);

    bool        Flush();
    bool        Truncate(bool sync = false);
    
    bool        ReadLittle32(uint32_t& x);
    bool        ReadLittle64(uint64_t& x);
    bool        ReadData(Buffer& data, unsigned len);

private:
    FD          fd;
    Buffer      buffer;
};

#endif
