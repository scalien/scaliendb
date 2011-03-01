#include "TestFunction.h"

#define TEST_LOG_TRACE   false

TEST_START(TestMain);
TEST_LOG_INIT(TEST_LOG_TRACE, LOG_TARGET_STDOUT);
//TEST_ADD(TestArrayListBasic);
//TEST_ADD(TestArrayListRemove);
//TEST_ADD(TestClientActivateNode);
//TEST_ADD(TestClientAdd);
//TEST_ADD(TestClientAppend);
//TEST_ADD(TestClientBasic);
//TEST_ADD(TestClientBatchedDelete);
//TEST_ADD(TestClientBatchedDummy);
//TEST_ADD(TestClientBatchedGet);
//TEST_ADD(TestClientBatchedGet2);
//TEST_ADD(TestClientBatchedSet);
//TEST_ADD(TestClientBatchedSet2);
//TEST_ADD(TestClientBatchedSetRandom);
//TEST_ADD(TestClientBatchLimit);
//TEST_ADD(TestClientCreateTable);
//TEST_ADD(TestClientFailover);
TEST_ADD(TestClientFilter);
TEST_ADD(TestClientFilter2);
//TEST_ADD(TestClientGet);
//TEST_ADD(TestClientGetAndSet);
//TEST_ADD(TestClientGetLatency);
//TEST_ADD(TestClientListKeys);
//TEST_ADD(TestClientMaro);
//TEST_ADD(TestClientMultiThread);
//TEST_ADD(TestClientMultiThreadMulti);
//TEST_ADD(TestClientSchemaSet);
//TEST_ADD(TestClientSet);
//TEST_ADD(TestCommonHumanBytes);
//TEST_ADD(TestCommonRandomDistribution)
//TEST_ADD(TestCrashStorage);
//TEST_ADD(TestFileSystemDiskSpace);
//TEST_ADD(TestFormattingUnsigned);
//TEST_ADD(TestInTreeMap);
//TEST_ADD(TestInTreeMapInsert);
//TEST_ADD(TestInTreeMapInsertRandom);
//TEST_ADD(TestInTreeMapRemoveRandom);
//TEST_ADD(TestManualBasic);
//TEST_ADD(TestSnprintfTiming);
//TEST_ADD(TestStorageAsyncList);
//TEST_ADD(TestStorage0DeleteTestDatabase);
//TEST_ADD(TestStorageAppend);
//TEST_ADD(TestStorageBasic);
//TEST_ADD(TestStorageBigRandomTransaction);
//TEST_ADD(TestStorageBigTransaction);
//TEST_ADD(TestStorageBinaryData);
//TEST_ADD(TestStorageCapacity);
//TEST_ADD(TestStorageBulkCursor);
//TEST_ADD(TestStorageCursor);
//TEST_ADD(TestStoragePageSplit);
//TEST_ADD(TestStorageFileSplit);
//TEST_ADD(TestStorageFileThreeWaySplit);
//TEST_ADD(TestStorageRandomGetSetDelete);
//TEST_ADD(TestStorageShardSize);         // TODO: this creates too much shards!
//TEST_ADD(TestStorageShardSplit);
//TEST_ADD(TestStorageWindowsSync);
//TEST_ADD(TestTimingBasicWrite);
//TEST_ADD(TestTimingSnprintf);
//TEST_ADD(TestTimingWrite);
TEST_EXECUTE();

int main()
{
    return TestMain();
}
