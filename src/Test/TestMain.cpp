#include "TestFunction.h"

#define TEST_LOG_TRACE   false

TEST_START(TestMain);
TEST_LOG_INIT(TEST_LOG_TRACE, LOG_TARGET_STDOUT|LOG_TARGET_FILE);
TEST_ADD(TestArrayListBasic);
TEST_ADD(TestArrayListRemove);
TEST_ADD(TestClientAdd);
TEST_ADD(TestClientBasic);
TEST_ADD(TestClientBatchedDelete);
TEST_ADD(TestClientBatchedSet);
TEST_ADD(TestClientBatchedSet2);
TEST_ADD(TestClientBatchedSetBulk);
TEST_ADD(TestClientBatchedSetRandom);
TEST_ADD(TestClientBatchLimit);
TEST_ADD(TestClientCount);
TEST_ADD(TestClientCreateSchema);
TEST_ADD(TestClientDistributeList);
TEST_ADD(TestClientAddFailover);
TEST_ADD(TestClientMaro);
TEST_ADD(TestClientGetFailover);
TEST_ADD(TestClientGetLatency);
TEST_ADD(TestClientGetSetGetSetSubmit);
TEST_ADD(TestClientInfiniteLoop);
TEST_ADD(TestClientListKeys);
TEST_ADD(TestClientListKeysWithPrefix);
TEST_ADD(TestClientMixedReadWriteBatched);
TEST_ADD(TestClientMixedWriteReadBatched);
TEST_ADD(TestClientMultiThread);
TEST_ADD(TestClientMultiThreadMulti);
TEST_ADD(TestClientSet);
TEST_ADD(TestClientSetFailover);
TEST_ADD(TestClientTruncateTable);
TEST_ADD(TestCommonHumanBytes);
TEST_ADD(TestCommonRandomDistribution);
TEST_ADD(TestCommonUInt64ToBufferWithBase);
TEST_ADD(TestConfigStateJSON);
TEST_ADD(TestEndpointValidity);
TEST_ADD(TestFileSystemDiskSpace);
TEST_ADD(TestFileSystemFileSize);
TEST_ADD(TestFormattingUnsigned);
TEST_ADD(TestFormattingPadding);
TEST_ADD(TestInTreeMap);
TEST_ADD(TestInTreeMapInsert);
TEST_ADD(TestInTreeMapInsertRandom);
TEST_ADD(TestInTreeMapMidpoint);
TEST_ADD(TestInTreeMapRemoveRandom);
TEST_ADD(TestJSONReaderBasic1);
TEST_ADD(TestJSONReaderBasic2);
TEST_ADD(TestJSONReaderBasic3);
TEST_ADD(TestJSONReaderBasic4);
TEST_ADD(TestJSONReaderBasic5);
TEST_ADD(TestJSONReaderBasic6);
TEST_ADD(TestJSONReaderBasic7);
TEST_ADD(TestJSONBasic);
TEST_ADD(TestJSONAdvanced);
TEST_ADD(TestLogRotate);
TEST_ADD(TestLogRotateMultiThreaded);
TEST_ADD(TestManualBasic);
TEST_ADD(TestMemoryOutOfMemoryError);
TEST_ADD(TestShardExtensionBasic);
TEST_ADD(TestStorageAsyncList);
TEST_ADD(TestStorageSet);
TEST_ADD(TestTimingBasicWrite);
TEST_ADD(TestTimingSnprintf);
TEST_ADD(TestTimingFileSystemWrite);
TEST_ADD(TestTimingFileSystemParallelWrite);
TEST_ADD(TestTimingWrite);
TEST_EXECUTE();

int main(int argc, char* argv[])
{
    return TestMain(argc, argv);
}
