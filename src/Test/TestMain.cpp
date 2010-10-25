#include "TestFunction.h"

TEST_START(TestMain);
//TEST_LOG_INIT(LOG_TARGET_STDOUT);
//TEST_ADD(TestStorageDeleteTestDatabase);
//TEST_ADD(TestStorage);
//TEST_ADD(TestStorageCapacity);
//TEST_ADD(TestStorageBigTransaction);
//TEST_ADD(TestStorageBigRandomTransaction);
//TEST_ADD(TestStorageShardSize);
//TEST_ADD(TestStorageShardSplit);
//TEST_ADD(TestStorageFileSplit);
//TEST_ADD(TestStorageFileThreeWaySplit);
//TEST_ADD(TestInTreeMap);
//TEST_ADD(TestInTreeMapInsert);
//TEST_ADD(TestInTreeMapInsertRandom);
//TEST_ADD(TestInTreeMapRemoveRandom);
//TEST_ADD(TestWriteTiming);
//TEST_ADD(TestFileSystemDiskSpace);
//TEST_ADD(TestArrayListBasic);
//TEST_ADD(TestArrayListRemove);
//TEST_ADD(TestClientBasic);
TEST_ADD(TestClientSet);
//TEST_ADD(TestClientBatchedSet);
//TEST_ADD(TestClientCreateTable);
//TEST_ADD(TestClientMaro);
//TEST_ADD(TestClientAdd);
TEST_EXECUTE();

#ifdef TEST
int main()
{
    return TestMain();
}
#endif