#ifndef STORAGERECOVERYLOG_H
#define STORAGERECOVERYLOG_H

#include "System/IO/FD.h"
#include "System/Platform.h"
#include "System/Buffers/Buffer.h"
#include "System/Events/Callable.h"

#define RECOVERY_OP_DONE        0
#define RECOVERY_OP_PAGE        1
#define RECOVERY_OP_FILE        2
#define RECOVERY_OP_COMMIT      3

/*
===============================================================================

 StorageRecoveryLog - used for reading and writing the recovery log

===============================================================================
*/

class StorageRecoveryLog
{
public:
    bool        Open(const char* filename);
    bool        Close();
    int64_t     GetFileSize();
    const char* GetFilename();

    bool        WriteOp(uint32_t op, uint32_t dataSize, Buffer& buffer);
    bool        WriteDone();

    bool        Truncate(bool sync = false);
    
    bool        Check(uint32_t length);
    bool        PerformRecovery(Callable callback);
    uint32_t    GetOp();
    uint32_t    GetDataSize();
    int32_t     ReadOpData(Buffer& buffer);
    void        Fail();
    
private:
    FD          fd;
    Buffer      name;

    // read
    Callable    callback;
    uint64_t    length;
    uint32_t    op;
    uint32_t    dataSize;
    bool        failed;
};

#endif
