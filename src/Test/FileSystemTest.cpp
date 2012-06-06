#include "Test.h"
#include "System/FileSystem.h"
#include "System/Common.h"

TEST_DEFINE(TestFileSystemDiskSpace)
{
    int64_t     total, free;
    const char  path[] = "/";
    char        humanTotalBuf[5];
    char        humanFreeBuf[5];
    
    total = FS_DiskSpace(path);
    free = FS_FreeDiskSpace(path);

    TEST_LOG("Total space: %s, free space: %s", HumanBytes(total, humanTotalBuf), HumanBytes(free, humanFreeBuf));

    return TEST_SUCCESS;
}

TEST_DEFINE(TestFileSystemFileSize)
{
    int64_t     size;
    const char  path[] = "d:\\bigfile";
    
    size = FS_FileSize(path);
    TEST_ASSERT(size == 8400355328);

    return TEST_SUCCESS;
}

TEST_DEFINE(TestFileSystemTruncate)
{
    char        filename[16 + 1];
    const char  set[] = "0123456789ABCDEF";
    FD          fd;
    bool        ret;
    ssize_t     numWritten;
    int64_t     fileSize;

    Log_SetTrace(true);

    // Generate random filename
    RandomBufferFromSet(filename, sizeof(filename) - 1, set, sizeof(set) - 1);
    filename[sizeof(filename) - 1] = 0;

    // Make sure the file not exists before we do anything with it
    if (FS_Exists(filename))
    {
        ret = FS_Delete(filename);
        TEST_ASSERT(ret);
    }

    // Open file and write some data in it ////////////////////////////////////////////////////////
    fd = FS_Open(filename,  FS_CREATE | FS_WRITEONLY | FS_APPEND);
    TEST_ASSERT(fd != INVALID_FD);

    numWritten = FS_FileWrite(fd, set, sizeof(set));
    TEST_ASSERT(numWritten == sizeof(set));

    FS_FileClose(fd);

    fileSize = FS_FileSize(filename);
    TEST_ASSERT(fileSize == numWritten);

    // Open existing file with truncate ///////////////////////////////////////////////////////////
    fd = FS_Open(filename,  FS_CREATE | FS_WRITEONLY | FS_APPEND | FS_TRUNCATE);
    TEST_ASSERT(fd != INVALID_FD);

    numWritten = FS_FileWrite(fd, set, sizeof(set));
    TEST_ASSERT(numWritten == sizeof(set));

    FS_FileClose(fd);

    fileSize = FS_FileSize(filename);
    TEST_ASSERT(fileSize == numWritten);

    // Delete file
    ret = FS_Delete(filename);
    TEST_ASSERT(ret);

    // Create new file with truncate //////////////////////////////////////////////////////////////
    fd = FS_Open(filename,  FS_CREATE | FS_WRITEONLY | FS_APPEND | FS_TRUNCATE);
    TEST_ASSERT(fd != INVALID_FD);

    numWritten = FS_FileWrite(fd, set, sizeof(set));
    TEST_ASSERT(numWritten == sizeof(set));

    FS_FileClose(fd);

    fileSize = FS_FileSize(filename);
    TEST_ASSERT(fileSize == numWritten);

    // Delete file
    ret = FS_Delete(filename);
    TEST_ASSERT(ret);

    return TEST_SUCCESS;
}
