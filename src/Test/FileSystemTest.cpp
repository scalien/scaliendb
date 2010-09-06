#include "Test.h"
#include "System/FileSystem.h"
#include "System/Common.h"

TEST_DEFINE(TestFileSystemDiskSpace)
{
	int64_t		total, free;
	const char	path[] = "/";
	
	total = FS_DiskSpace(path);
	free = FS_FreeDiskSpace(path);

	TEST_LOG("Total space: %s, free space: %s", HumanBytes(total), HumanBytes(free));

	return TEST_SUCCESS;
}

TEST_MAIN(TestFileSystemDiskSpace);