#include "Test.h"
#include "System/FileSystem.h"
#include "System/Common.h"

TEST_DEFINE(TestFileSystemDiskSpace)
{
    int64_t     total, free;
    const char  path[] = "/";
    
    total = FS_DiskSpace(path);
    free = FS_FreeDiskSpace(path);

    TEST_LOG("Total space: %s, free space: %s", HUMAN_BYTES(total), HUMAN_BYTES(free));

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

TEST_MAIN(TestFileSystemDiskSpace);