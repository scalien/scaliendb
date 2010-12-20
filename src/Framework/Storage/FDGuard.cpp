#include "FDGuard.h"
#include "System/FileSystem.h"

FDGuard::FDGuard()
{
    fd = INVALID_FD;
}

FDGuard::~FDGuard()
{
    if (fd != INVALID_FD)
        FS_FileClose(fd);
}

FD FDGuard::Open(const char* filename, int mode)
{
    fd = FS_Open(filename, mode);
    return fd;
}

void FDGuard::Close()
{
    FS_FileClose(fd);
    fd = INVALID_FD;
}

FD FDGuard::GetFD()
{
    return fd;
}
