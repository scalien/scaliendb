#include "Test.h"

TEST_DECLARE(TestStorage);
TEST_DECLARE(TestStorageCapacity);
TEST_DECLARE(TestStorageBigTransaction);
TEST_DECLARE(TestStorageBigRandomTransaction);
TEST_DECLARE(TestStorageShardSize);
TEST_DECLARE(TestStorageShardSplit);
TEST_DECLARE(TestInTreeMap);
TEST_DECLARE(TestInTreeMapInsert);
TEST_DECLARE(TestInTreeMapInsertRandom);
TEST_DECLARE(TestInTreeMapRemoveRandom);
TEST_DECLARE(TestWriteTiming);
TEST_DECLARE(TestFileSystemDiskSpace);


TEST_DEFINE(TestMain)
{
	TEST_RUN(
		//TestInTreeMap,
		//TestInTreeMapInsert,
		//TestInTreeMapInsertRandom,
		//TestInTreeMapRemoveRandom,
		//TestStorage,
		//TestStorageCapacity,
		//TestStorageBigTransaction,
		//TestStorageBigRandomTransaction,
		//TestStorageShardSize,
		TestStorageShardSplit,
		//TestWriteTiming,
		//TestFileSystemDiskSpace,
	);
}

int main()
{
	return TestMain();
}
