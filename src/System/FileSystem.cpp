#include "FileSystem.h"
#include "Log.h"

#include <string.h>

/*
===============================================================================================

 Platform independent functionality

===============================================================================================
*/

bool FS_IsSpecial(const char* path)
{
    size_t  len;
    
    len = strlen(path);

    if (len == 1 && path[0] == '.')
        return true;
    if (len == 2 && path[0] == '.' && path[1] == '.')
        return true;
    
    return false;
}

bool FS_RecDeleteDir(const char* path)
{
    FS_Dir      dir;
    FS_DirEntry entry;
    bool        ret;
    const char* dirname;
    size_t      pathlen;
    size_t      tmplen;
    char*       tmp;
    
    if (!FS_IsDirectory(path))
        return false;

    if (FS_IsSpecial(path))
        return false;
    
    dir = FS_OpenDir(path);
    if (dir == FS_INVALID_DIR)
        return false;
    
    pathlen = strlen(path);
    while ((entry = FS_ReadDir(dir)) != FS_INVALID_DIR_ENTRY)
    {
        dirname = FS_DirEntryName(entry);
        if (FS_IsSpecial(dirname))
            continue;
        
        // create relative path in temporary buffer
        tmplen = pathlen + 1 + strlen(dirname) + 1;
        tmp = new char[tmplen];
        memcpy(tmp, path, pathlen);
        tmp[pathlen] = FS_Separator();
        memcpy(tmp + pathlen + 1, dirname, strlen(dirname));
        tmp[tmplen - 1] = '\0';
        
        if (FS_IsDirectory(tmp))
            ret = FS_RecDeleteDir(tmp);
        else
            ret = FS_Delete(tmp);

        delete[] tmp;
        if (!ret)
            return false;
    }
    
    FS_CloseDir(dir);
    return FS_DeleteDir(path);
}

/*
===============================================================================================

 Unix compatible filesystem

===============================================================================================
*/

#ifndef PLATFORM_WINDOWS
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/uio.h>
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>
#include <alloca.h>
#include <stdio.h>

FD FS_Open(const char* filename, int flags)
{
    int     mode;
    int     oflags;
    int     ret;
    
    mode = S_IRUSR | S_IWUSR;
    oflags = 0;
    if ((flags & FS_CREATE) == FS_CREATE)
        oflags |= O_CREAT;
    if ((flags & FS_READWRITE) == FS_READWRITE)
        oflags |= O_RDWR;
    if ((flags & FS_READONLY) == FS_READONLY)
        oflags |= O_RDONLY;

    ret = open(filename, oflags, mode);
    if (ret < 0)
    {
        Log_Errno();
        return INVALID_FD;
    }
    return ret;
}

void FS_FileClose(FD fd)
{
    int ret;
    
    ret = close(fd);
    if (ret < 0)
        Log_Errno();
}

int64_t FS_FileSeek(FD fd, uint64_t offset, int whence_)
{
    off_t   ret;
    int     whence;
    
    whence = -1;
    switch (whence_)
    {
    case FS_SEEK_SET:
        whence = SEEK_SET;
        break;
    case FS_SEEK_CUR:
        whence = SEEK_CUR;
        break;
    case FS_SEEK_END:
        whence = SEEK_END;
        break;
    default:
        return -1;
    }
    
    ret = lseek(fd, offset, whence);
    if (ret < 0)
        Log_Errno();
    return ret;
}

int FS_FileTruncate(FD fd, uint64_t length)
{
    int ret;
    
    ret = ftruncate(fd, length);
    if (ret < 0)
        Log_Errno();
    return ret;
}

int64_t FS_FileSize(FD fd)
{
    int64_t     ret;
    struct stat buf;
    
    ret = fstat(fd, &buf);
    if (ret < 0)
    {
        Log_Errno();
        return ret;
    }
    
    return buf.st_size;
}

ssize_t FS_FileWrite(FD fd, const void* buf, size_t count)
{
    ssize_t ret;
    
    ret = write(fd, buf, count);
    if (ret < 0)
        Log_Errno();
    return ret;
}

ssize_t FS_FileWriteVector(FD fd, unsigned num, const void** buf, size_t *count)
{
    ssize_t         ret;
    struct iovec    vecbuf[num];
    unsigned        i;
    
    for (i = 0; i < num; i++)
    {
        vecbuf[i].iov_base = (void*) buf[i];
        vecbuf[i].iov_len = count[i];
    }
    
    ret = writev(fd, vecbuf, num);
    if (ret < 0)
        Log_Errno();
    return ret; 
}

ssize_t FS_FileRead(FD fd, void* buf, size_t count)
{
    ssize_t ret;
    
    ret = read(fd, buf, count);
    if (ret < 0)
        Log_Errno();
    return ret;
}

ssize_t FS_FileWriteOffs(FD fd, const void* buf, size_t count, uint64_t offset)
{
    ssize_t ret;
    
    ret = pwrite(fd, buf, count, offset);
    if (ret < 0)
        Log_Errno();
    return ret;
}

ssize_t FS_FileReadOffs(FD fd, void* buf, size_t count, uint64_t offset)
{
    ssize_t ret;
    
    ret = pread(fd, buf, count, offset);
    if (ret < 0)
        Log_Errno();
    return ret;
}

bool FS_Delete(const char* filename)
{
    int ret;
    
    ret = unlink(filename);
    if (ret < 0)
    {
        Log_Errno();
        return false;
    }
    
    return true;
}

FS_Dir FS_OpenDir(const char* filename)
{
    DIR*    dir;
    
    dir = opendir(filename);
    if (dir == NULL)
    {
        Log_Errno();
        return FS_INVALID_DIR;
    }
    
    return (FS_Dir) dir;
}

FS_DirEntry FS_ReadDir(FS_Dir dir)
{
    if ((DIR*) dir == NULL)
        return FS_INVALID_DIR_ENTRY;
    return (FS_DirEntry) readdir((DIR*) dir);
}

void FS_CloseDir(FS_Dir dir)
{
    int ret;
    
    if ((DIR*) dir == NULL)
        return;
        
    ret = closedir((DIR*) dir);
    if (ret < 0)
        Log_Errno();
}

bool FS_CreateDir(const char* filename)
{
    int ret;
    
    ret = mkdir(filename, S_IRUSR | S_IWUSR | S_IXUSR);
    if (ret < 0)
    {
        Log_Errno();
        return false;
    }
    return true;
}

bool FS_DeleteDir(const char* filename)
{
    int ret;
    
    ret = rmdir(filename);
    if (ret < 0)
    {
        Log_Errno();
        return false;
    }
    return true;
}

const char* FS_DirEntryName(FS_DirEntry dirent)
{
    return ((struct dirent*) dirent)->d_name;
}

bool FS_IsFile(const char* path)
{
    struct stat s;
    if (stat(path, &s) != 0)
        return false;
    if (s.st_mode & S_IFREG)
        return true;
    return false;
}

bool FS_IsDirectory(const char* path)
{
    struct stat s;
    if (stat(path, &s) != 0)
        return false;
    if (s.st_mode & S_IFDIR)
        return true;
    return false;
}

bool FS_Exists(const char* path)
{
    struct stat s;
    if (stat(path, &s) != 0)
        return false;
    return true;
}

int64_t FS_FreeDiskSpace(const char* path)
{
    struct statvfs sv;
    
    if (statvfs(path, &sv) < 0)
        return -1;
    
    return ((int64_t) sv.f_bavail * sv.f_frsize);
}

int64_t FS_DiskSpace(const char* path)
{
    struct statvfs sv;
    
    if (statvfs(path, &sv) < 0)
        return -1;
    
    return ((int64_t) sv.f_blocks * sv.f_frsize);
}

int64_t FS_FileSize(const char* path)
{
    int64_t     ret;
    struct stat buf;
    
    ret = stat(path, &buf);
    if (ret < 0)
    {
        Log_Errno();
        return ret;
    }
    
    return buf.st_size;
}

bool FS_Rename(const char* src, const char* dst)
{
    int     ret;
    
    ret = rename(src, dst);
    if (ret < 0)
    {
        Log_Errno();
        return false;
    }
    
    return true;
}

void FS_Sync()
{
    sync();
}

char FS_Separator()
{
    return '/';
}

#else

/*
===============================================================================================

 Windows filesystem

===============================================================================================
*/

#include <windows.h>

struct FS_Dir_Windows
{
    HANDLE              handle;
    WIN32_FIND_DATA     findData;
};

FD FS_Open(const char* filename, int flags)
{
    FD      fd;
    DWORD   dwCreationDisposition;
    DWORD   dwDesiredAccess;
    DWORD   dwFlagsAndAttributes;
    HANDLE  handle;
    
    dwCreationDisposition = OPEN_EXISTING;
    if ((flags & FS_CREATE) == FS_CREATE)
        dwCreationDisposition = OPEN_ALWAYS;
    
    dwDesiredAccess = 0;
    if ((flags & FS_READONLY) == FS_READONLY)
        dwDesiredAccess = GENERIC_READ;
    if ((flags & FS_READWRITE) == FS_READWRITE)
        dwDesiredAccess = GENERIC_READ | GENERIC_WRITE;
    
    // TODO: unbuffered file IO
    dwFlagsAndAttributes = FILE_ATTRIBUTE_ARCHIVE /* | FILE_FLAG_NO_BUFFERING */;
    
    handle = CreateFile(filename, dwDesiredAccess, FILE_SHARE_READ, NULL, 
     dwCreationDisposition, dwFlagsAndAttributes, NULL);
    
    if (handle == INVALID_HANDLE_VALUE)
    {
        DWORD   error;

        error = GetLastError();
        Log_Message("error = %u", error);
        fd = INVALID_FD;
    }
    
    fd.handle = (intptr_t) handle;
    return fd;
}

void FS_FileClose(FD fd)
{
    BOOL    ret;
    
    ret = CloseHandle((HANDLE)fd.handle);
    if (!ret)
        Log_Errno();
}

int64_t FS_FileSeek(FD fd, uint64_t offset, int whence_)
{
    BOOL            ret;
    DWORD           dwMoveMethod;
    LARGE_INTEGER   distanceToMove;
    LARGE_INTEGER   newFilePointer;

    distanceToMove.QuadPart = offset;
    newFilePointer.QuadPart = 0;

    switch (whence_)
    {
    case FS_SEEK_SET:
        dwMoveMethod = FILE_BEGIN;
        break;
    case FS_SEEK_CUR:
        dwMoveMethod = FILE_CURRENT;
        break;
    case FS_SEEK_END:
        dwMoveMethod = FILE_END;
        break;
    default:
        return -1;
    }
    
    ret = SetFilePointerEx((HANDLE)fd.handle, distanceToMove, &newFilePointer, dwMoveMethod);
    if (!ret)
    {
        Log_Errno();
        return -1;
    }
    
    return (int64_t) newFilePointer.QuadPart;
}

int FS_FileTruncate(FD fd, uint64_t length)
{
    BOOL    ret;
    
    if (!FS_FileSeek(fd, length, FS_SEEK_SET))
        return -1;

    ret = SetEndOfFile((HANDLE)fd.handle);
    if (!ret)
    {
        Log_Errno();
        return -1;
    }
    
    return 0;
}

int64_t FS_FileSize(FD fd)
{
    BOOL            ret;
    LARGE_INTEGER   fileSize;

    ret = GetFileSizeEx((HANDLE)fd.handle, &fileSize);
    if (!ret)
    {
        Log_Errno();
        return -1;
    }
    
    return fileSize.QuadPart;
}

ssize_t FS_FileWrite(FD fd, const void* buf, size_t count)
{
    BOOL    ret;
    DWORD   numWritten;
    
    ret = WriteFile((HANDLE)fd.handle, buf, (DWORD) count, &numWritten, NULL);
    if (!ret)
    {
        Log_Errno();
        return -1;
    }
    
    return (ssize_t) numWritten;
}

ssize_t FS_FileRead(FD fd, void* buf, size_t count)
{
    BOOL    ret;
    DWORD   numRead;
    
    ret = ReadFile((HANDLE)fd.handle, buf, (DWORD) count, &numRead, NULL);
    if (!ret)
    {
        Log_Errno();
        return -1;
    }
    return (ssize_t) numRead;
}

ssize_t FS_FileWriteOffs(FD fd, const void* buf, size_t count, uint64_t offset)
{
    BOOL        ret;
    DWORD       numWritten;
    OVERLAPPED  overlapped;
    
    overlapped.hEvent = NULL;
    overlapped.Internal = 0;
    overlapped.InternalHigh = 0;
    overlapped.Offset = offset & 0xFFFFFFFF;
    overlapped.OffsetHigh = offset >> 32;
    ret = WriteFile((HANDLE)fd.handle, buf, (DWORD) count, &numWritten, &overlapped);
    if (!ret)
    {
        Log_Errno();
        return -1;
    }
    
    return (ssize_t) numWritten;
}

ssize_t FS_FileReadOffs(FD fd, void* buf, size_t count, uint64_t offset)
{
    BOOL        ret;
    DWORD       numRead;
    OVERLAPPED  overlapped;
    
    overlapped.hEvent = NULL;
    overlapped.Internal = 0;
    overlapped.InternalHigh = 0;
    overlapped.Offset = offset & 0xFFFFFFFF;
    overlapped.OffsetHigh = offset >> 32;
    ret = ReadFile((HANDLE)fd.handle, buf, (DWORD) count, &numRead, &overlapped);
    if (!ret)
    {
        Log_Errno();
        return -1;
    }
    return (ssize_t) numRead;
}

bool FS_Delete(const char* filename)
{
    BOOL    ret;
    
    ret = DeleteFile(filename);
    if (!ret)
    {
        Log_Errno();
        return false;
    }
    
    return true;
}

FS_Dir FS_OpenDir(const char* filename)
{
    FS_Dir_Windows*     dir;
    size_t              len;
    char                path[MAX_PATH];
    
    // The filename parameter to FindFirstFile should not be NULL, an invalid string 
	// (for example, an empty string or a string that is missing the terminating 
	// null character), or end in a trailing backslash (\).
    // http://msdn.microsoft.com/en-us/library/aa364418(VS.85).aspx
	if (filename == NULL)
		return FS_INVALID_DIR;
	
	len = strlen(filename);
    if (len == 0)
        return FS_INVALID_DIR;

    if (len >= MAX_PATH)
        return FS_INVALID_DIR;

    if (filename[len - 1] == '\\')
    {
        memcpy(path, filename, len - 1);
        path[len - 1] = '\0';
        filename = path;
    }

    dir = new FS_Dir_Windows;
    dir->handle = FindFirstFile(filename, &dir->findData);
    if (dir->handle == INVALID_HANDLE_VALUE)
    {
        delete dir;
        Log_Errno();
        return FS_INVALID_DIR;
    }
    
    return (FS_Dir) dir;
}

FS_DirEntry FS_ReadDir(FS_Dir dir_)
{
    FS_Dir_Windows*     dir;
    
    dir = (FS_Dir_Windows*) dir_;
    if (dir == NULL)
        return FS_INVALID_DIR_ENTRY;
    if (!FindNextFile(dir->handle, &dir->findData))
        return FS_INVALID_DIR_ENTRY;
    
    return (FS_DirEntry) &dir->findData;
}

void FS_CloseDir(FS_Dir dir_)
{
    BOOL                ret;
    FS_Dir_Windows*     dir;
    
    dir = (FS_Dir_Windows*) dir_;
    if (dir == NULL)
        return;
    
    ret = FindClose(dir->handle);
    delete dir;

    if (!ret)
        Log_Errno();
}

bool FS_CreateDir(const char* filename)
{
    BOOL    ret;
    
    ret = CreateDirectory(filename, NULL);
    if (!ret)
    {
        Log_Errno();
        return false;
    }
    return true;
}

bool FS_DeleteDir(const char* filename)
{
    BOOL    ret;
    
    ret = RemoveDirectory(filename);
    if (!ret)
    {
        Log_Errno();
        return false;
    }
    return true;
}

const char* FS_DirEntryName(FS_DirEntry dirent)
{
    return ((WIN32_FIND_DATA*) dirent)->cFileName;
}

bool FS_IsFile(const char* path)
{
    WIN32_FILE_ATTRIBUTE_DATA   attrData;
    BOOL                        ret;

    ret = GetFileAttributesEx(path, GetFileExInfoStandard, &attrData);
    if (!ret)
        return false;
    if ((attrData.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) == 0 &&
     (attrData.dwFileAttributes & FILE_ATTRIBUTE_DEVICE) == 0)
        return true;

    return false;
}

bool FS_IsDirectory(const char* path)
{
    WIN32_FILE_ATTRIBUTE_DATA   attrData;
    BOOL                        ret;

    ret = GetFileAttributesEx(path, GetFileExInfoStandard, &attrData);
    if (!ret)
        return false;
    if ((attrData.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) == FILE_ATTRIBUTE_DIRECTORY)
        return true;

    return false;
}

bool FS_Exists(const char* path)
{
    WIN32_FILE_ATTRIBUTE_DATA   attrData;
    BOOL                        ret;

    ret = GetFileAttributesEx(path, GetFileExInfoStandard, &attrData);
    if (!ret)
        return false;
    return true;
}

int64_t FS_FreeDiskSpace(const char* path)
{
    ULARGE_INTEGER  bytes;
    
    if (!GetDiskFreeSpaceEx(path, &bytes, NULL, NULL))
        return -1;
    return bytes.QuadPart;
}

int64_t FS_DiskSpace(const char* path)
{
    ULARGE_INTEGER  bytes;
    
    if (!GetDiskFreeSpaceEx(path, NULL, &bytes, NULL))
        return -1;
    return bytes.QuadPart;
}

int64_t FS_FileSize(const char* path)
{
    WIN32_FILE_ATTRIBUTE_DATA   attrData;
    BOOL                        ret;

    ret = GetFileAttributesEx(path, GetFileExInfoStandard, &attrData);
    if (!ret)
        return -1;
    
    return (int64_t) attrData.nFileSizeHigh << 32 | attrData.nFileSizeLow;
}

bool FS_Rename(const char* src, const char* dst)
{
    BOOL    ret;
    
    ret = MoveFileEx(src, dst, MOVEFILE_WRITE_THROUGH);
    if (!ret)
    {
        Log_Errno();
        return false;
    }
    
    return true;
}

void FS_Sync()
{
    // TODO: To flush all open files on a volume, call FlushFileBuffers with a handle to the volume.
    // http://msdn.microsoft.com/en-us/library/aa364439(v=VS.85).aspx
}

char FS_Separator()
{
    return '\\';
}

#endif
