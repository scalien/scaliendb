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
    ST_ASSERT(version != 0);
    
    buffer.Allocate(STORAGEFILE_HEADER_LENGTH);
    buffer.Zero();

    buffer.Append(type, STORAGEFILE_TYPE_LENGTH);
    buffer.AppendLittle32(version);
    buffer.AppendLittle32(crc);

    buffer.SetLength(STORAGEFILE_HEADER_LENGTH);
    
    return true;
}

void StorageFileHeader::SetVersion(unsigned major, unsigned minor)
{
    version = (major & 0xFF) << 8 | (minor & 0xFF);
}
