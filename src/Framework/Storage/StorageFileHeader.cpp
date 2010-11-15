#include "StorageFileHeader.h"
#include "StorageDefaults.h"

StorageFileHeader::StorageFileHeader()
{
    memset(type, 0, STORAGEFILE_TYPE_LENGTH);
    version = 0;
    crc = 0;
}

void StorageFileHeader::Init(const char* type_, unsigned major, 
 unsigned minor, uint32_t crc_)
{
    size_t  len;
    
    memset(type, 0, STORAGEFILE_TYPE_LENGTH);
    len = strlen(type_);
    if (len >= STORAGEFILE_TYPE_LENGTH)
        len = STORAGEFILE_TYPE_LENGTH - 1;
    memcpy(type, type_, len);
    
    SetVersion(major, minor);
    
    crc = crc_;
}

bool StorageFileHeader::Read(Buffer& buffer)
{
    char*   p;

    if (buffer.GetLength() < STORAGEFILE_HEADER_LENGTH)
        return false;
    
    p = buffer.GetBuffer();
    memcpy(type, p, STORAGEFILE_TYPE_LENGTH);
    type[sizeof(type) - 1] = 0;
    
    p += STORAGEFILE_TYPE_LENGTH;
    version = FromLittle32(*((uint32_t*) p));
    
    p += STORAGEFILE_VERSION_LENGTH;
    crc = FromLittle32(*((uint32_t*) p));
    
    return true;
}

bool StorageFileHeader::Write(Buffer& buffer)
{
    size_t  len;
    char*   p;
    
    ST_ASSERT(version != 0);
    
    buffer.Allocate(STORAGEFILE_HEADER_LENGTH);
    buffer.SetLength(STORAGEFILE_HEADER_LENGTH);
    buffer.Zero();
    p = buffer.GetBuffer();

    // write type
    len = strlen(type);
    memcpy(p, type, len);
    
    // write version
    p += STORAGEFILE_TYPE_LENGTH;
    *((uint32_t*) p) = ToLittle32(version);
    
    // write CRC
    p += STORAGEFILE_VERSION_LENGTH;
    *((uint32_t*) p) = ToLittle32(crc);
    
    return true;
}

void StorageFileHeader::SetVersion(unsigned major, unsigned minor)
{
    version = (major & 0xFF) << 8 | (minor & 0xFF);
}
