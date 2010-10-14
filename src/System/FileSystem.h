#ifndef FILESYSTEM_H
#define FILESYSTEM_H

#include "Platform.h"
#include "IO/FD.h"

/*
===============================================================================

 FileSystem: platform-independent filesystem interface

===============================================================================
*/

#define FS_SEEK_SET             0
#define FS_SEEK_CUR             1
#define FS_SEEK_END             2

#define FS_CREATE               0x0200
#define FS_READONLY             0x0000
#define FS_READWRITE            0x0002

#define FS_INVALID_DIR          0
#define FS_INVALID_DIR_ENTRY    0

typedef intptr_t FS_Dir;
typedef intptr_t FS_DirEntry;

FD          FS_Open(const char* filename, int mode);
void        FS_FileClose(FD fd);
int64_t     FS_FileSeek(FD fd, uint64_t offset, int whence);
int         FS_FileTruncate(FD fd, uint64_t length);
int64_t     FS_FileSize(FD fd);
ssize_t     FS_FileWrite(FD fd, const void* buf, size_t count);
ssize_t     FS_FileWriteVector(FD fd, unsigned num, const void** buf, size_t *count);
ssize_t     FS_FileRead(FD fd, void* buf, size_t count);
ssize_t     FS_FileWriteOffs(FD fd, const void* buf, size_t count, uint64_t offset);
ssize_t     FS_FileReadOffs(FD fd, void* buf, size_t count, uint64_t offset);

bool        FS_Delete(const char* filename);

FS_Dir      FS_OpenDir(const char* filename);
FS_DirEntry FS_ReadDir(FS_Dir dir);
void        FS_CloseDir(FS_Dir dir);
bool        FS_CreateDir(const char* name);
bool        FS_DeleteDir(const char* name);
bool        FS_RecDeleteDir(const char* name);

const char* FS_DirEntryName(FS_DirEntry dirent);

bool        FS_IsFile(const char* name);
bool        FS_IsDirectory(const char* name);
int64_t     FS_FreeDiskSpace(const char* path);
int64_t     FS_DiskSpace(const char* path);
int64_t     FS_FileSize(const char* path);
bool        FS_Rename(const char* src, const char* dst);

void        FS_Sync();

char        FS_Separator();

#endif
