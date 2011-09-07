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
//TEST_ADD(TestClientBatchedSet);
//TEST_ADD(TestClientBatchedGet2);
//TEST_ADD(TestClientBatchedSet2);
//TEST_ADD(TestClientBatchedSetBulk);
//TEST_ADD(TestClientBatchedSetRandom);
//TEST_ADD(TestClientBatchLimit);
//TEST_ADD(TestClientCount);
//TEST_ADD(TestClientCreateTable);
//TEST_ADD(TestClientCreateSchema);
//TEST_ADD(TestClientDistributeList);
//TEST_ADD(TestClientGetFailover);
//TEST_ADD(TestClientAddFailover);
//TEST_ADD(TestClientFilter);
//TEST_ADD(TestClientFilter2);
//TEST_ADD(TestClientMaro);
//TEST_ADD(TestClientGetAndSet);
//TEST_ADD(TestClientGetLatency);
//TEST_ADD(TestClientListKeys);
//TEST_ADD(TestClientListKeysWithPrefix);
//TEST_ADD(TestClientMaro);
//TEST_ADD(TestClientMixedReadWriteBatched);
//TEST_ADD(TestClientMixedWriteReadBatched);
TEST_ADD(TestClientMultiThread);
//TEST_ADD(TestClientMultiThreadMulti);
//TEST_ADD(TestClientRemove);
//TEST_ADD(TestClientSchemaSet);
//TEST_ADD(TestClientSet);
//TEST_ADD(TestClientSetFailover);
//TEST_ADD(TestClientTestAndSet);
//TEST_ADD(TestClientTestAndDelete);
//TEST_ADD(TestCommonHumanBytes);
//TEST_ADD(TestCommonRandomDistribution)
//TEST_ADD(TestConfigStateJSON);
//TEST_ADD(TestCrashStorage);
//TEST_ADD(TestDLLLoad);
//TEST_ADD(TestEndpointValidity);
//TEST_ADD(TestFileSystemDiskSpace);
//TEST_ADD(TestFormattingUnsigned);
//TEST_ADD(TestFormattingPadding);
//TEST_ADD(TestInTreeMap);
//TEST_ADD(TestInTreeMapInsert);
//TEST_ADD(TestInTreeMapInsertRandom);
//TEST_ADD(TestInTreeMapMidpoint);
//TEST_ADD(TestInTreeMapRemoveRandom);
//TEST_ADD(TestJSONReaderBasic1);
//TEST_ADD(TestJSONReaderBasic2);
//TEST_ADD(TestJSONReaderBasic3);
//TEST_ADD(TestJSONReaderBasic4);
//TEST_ADD(TestJSONReaderBasic5);
//TEST_ADD(TestJSONReaderBasic6);
//TEST_ADD(TestJSONReaderBasic7);
//TEST_ADD(TestJSONBasic);
//TEST_ADD(TestJSONAdvanced);
//TEST_ADD(TestManualBasic);
//TEST_ADD(TestMemoryOutOfMemoryError);
//TEST_ADD(TestShardExtensionBasic);
//TEST_ADD(TestSnprintfTiming);
//TEST_ADD(TestStorageAsyncList);
//TEST_ADD(TestStorageSet);
//TEST_ADD(TestTimingBasicWrite);
//TEST_ADD(TestTimingSnprintf);
//TEST_ADD(TestTimingFileSystemWrite);
//TEST_ADD(TestTimingFileSystemParallelWrite);
//TEST_ADD(TestTimingWrite);
TEST_EXECUTE();

int main()
{
    return TestMain();
}
