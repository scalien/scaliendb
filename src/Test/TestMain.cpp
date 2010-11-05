#include "TestFunction.h"

TEST_START(TestMain);
//TEST_LOG_INIT(LOG_TARGET_STDOUT);
//TEST_ADD(TestStorageDeleteTestDatabase);
//TEST_ADD(TestStorage);
//TEST_ADD(TestStorageCapacity);
//TEST_ADD(TestStorageBigTransaction);
//TEST_ADD(TestStorageBigRandomTransaction);
//TEST_ADD(TestStorageShardSize);         // TODO: this creates too much shards!
//TEST_ADD(TestStorageShardSplit);
//TEST_ADD(TestStorageFileSplit);
//TEST_ADD(TestStorageFileThreeWaySplit);
//TEST_ADD(TestStorageBinaryData);
//TEST_ADD(TestInTreeMap);
//TEST_ADD(TestInTreeMapInsert);
//TEST_ADD(TestInTreeMapInsertRandom);
//TEST_ADD(TestInTreeMapRemoveRandom);
//TEST_ADD(TestWriteTiming);
//TEST_ADD(TestSnprintfTiming);
//TEST_ADD(TestFileSystemDiskSpace);
//TEST_ADD(TestArrayListBasic);
//TEST_ADD(TestArrayListRemove);
//TEST_ADD(TestClientSchemaSet);
//TEST_ADD(TestClientBasic);
//TEST_ADD(TestClientSet);
//TEST_ADD(TestClientBatchedSet);
//TEST_ADD(TestClientBatchedSetRandom);     // TODO: this crashes shard server various ways!
//TEST_ADD(TestClientBatchedGet);
//TEST_ADD(TestClientCreateTable);
//TEST_ADD(TestClientMaro);
//TEST_ADD(TestClientAdd);
//TEST_ADD(TestClientFailover);
//TEST_ADD(TestUnsignedFormatting);
//TEST_ADD(TestManualBasic);
TEST_ADD(TestCommonHumanBytes);
TEST_EXECUTE();

#ifdef TEST
int main()
{
    return TestMain();
}
#endif