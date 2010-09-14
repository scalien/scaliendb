#include "FileSystem.h"
#include "Log.h"

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
	int		mode;
	int		oflags;
	int		ret;
	
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
	int	ret;
	
	ret = close(fd);
	if (ret < 0)
		Log_Errno();
}

int64_t FS_FileSeek(FD fd, uint64_t offset, int whence_)
{
	off_t	ret;
	int		whence;
	
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
	int	ret;
	
	ret = ftruncate(fd, length);
	if (ret < 0)
		Log_Errno();
	return ret;
}

int64_t FS_FileSize(FD fd)
{
	int64_t		ret;
	struct stat	buf;
	
	ret = fstat(fd, &buf);
	if (ret < 0)
	{
		Log_Errno();
		return ret;
	}
	
	return buf.st_size;
}

ssize_t	FS_FileWrite(FD fd, const void* buf, size_t count)
{
	ssize_t	ret;
	
	ret = write(fd, buf, count);
	if (ret < 0)
		Log_Errno();
	return ret;
}

ssize_t	FS_FileWriteVector(FD fd, unsigned num, const void** buf, size_t *count)
{
	ssize_t			ret;
	struct iovec	vecbuf[num];
	unsigned		i;
	
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
	ssize_t	ret;
	
	ret = read(fd, buf, count);
	if (ret < 0)
		Log_Errno();
	return ret;
}

ssize_t	FS_FileWriteOffs(FD fd, const void* buf, size_t count, uint64_t offset)
{
	ssize_t	ret;
	
	ret = pwrite(fd, buf, count, offset);
	if (ret < 0)
		Log_Errno();
	return ret;
}

ssize_t FS_FileReadOffs(FD fd, void* buf, size_t count, uint64_t offset)
{
	ssize_t	ret;
	
	ret = pread(fd, buf, count, offset);
	if (ret < 0)
		Log_Errno();
	return ret;
}

bool FS_Delete(const char* filename)
{
	int	ret;
	
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
	DIR*	dir;
	
	dir = opendir(filename);
	if (dir == NULL)
	{
		Log_Errno();
		return NULL;
	}
	
	return (FS_Dir) dir;
}

FS_DirEntry	FS_ReadDir(FS_Dir dir)
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
	int	ret;
	
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
	int	ret;
	
	ret = rmdir(filename);
	if (ret < 0)
	{
		Log_Errno();
		return false;
	}
	return true;
}

const char*	FS_DirEntryName(FS_DirEntry dirent)
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

int64_t	FS_FreeDiskSpace(const char* path)
{
	struct statvfs sv;
	
	if (statvfs(path, &sv) < 0)
		return -1;
	
	return ((int64_t) sv.f_bavail * sv.f_frsize);
}

int64_t	FS_DiskSpace(const char* path)
{
	struct statvfs sv;
	
	if (statvfs(path, &sv) < 0)
		return -1;
	
	return ((int64_t) sv.f_blocks * sv.f_frsize);
}

int64_t FS_FileSize(const char* path)
{
	int64_t		ret;
	struct stat	buf;
	
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
	int		ret;
	
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

// TODO: Windows port

int64_t	FS_FreeDiskSpace(const char* path)
{
	ULARGE_INTEGER	bytes;
	
	if (!GetDiskFreeSpaceEx(path, &bytes, NULL, NULL))
		return -1;
	return bytes.QuadPart;
}

#endif
