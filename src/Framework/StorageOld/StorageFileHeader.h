#ifndef STORAGEFILEHEADER_H
#define STORAGEFILEHEADER_H

#include "System/Buffers/Buffer.h"
#include "System/Platform.h"
#include "StorageIndexPage.h"

#define STORAGEFILE_TYPE_LENGTH         32
#define STORAGEFILE_VERSION_LENGTH      4
#define STORAGEFILE_CRC_LENGTH          4
#define STORAGEFILE_UNDEFINED_LENGTH    (STORAGEFILE_HEADER_LENGTH - \
                                        STORAGEFILE_TYPE_LENGTH - \
                                        STORAGEFILE_VERSION_LENGTH - \
                                        STORAGEFILE_CRC_LENGTH)

#define STORAGEFILE_HEADER_LENGTH       (4096 - INDEXPAGE_HEADER_SIZE)

/*
===============================================================================

 StorageFileHeader

===============================================================================
*/

class StorageFileHeader
{
public:
    StorageFileHeader();

    void        Init(const char* type, unsigned major, unsigned minor, uint32_t crc);

    bool        Read(Buffer& buffer);
    bool        Write(Buffer& buffer);
    
    char        type[STORAGEFILE_TYPE_LENGTH];
    uint32_t    version;
    uint32_t    crc;

private:
    void        SetVersion(unsigned major, unsigned minor);
};

#endif
