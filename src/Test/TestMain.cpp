#include "Test.h"

TEST_DECLARE(TestStorage);
TEST_DECLARE(TestStorageCapacity);
TEST_DECLARE(TestStorageBigTransaction);
TEST_DECLARE(TestStorageBigRandomTransaction);
TEST_DECLARE(TestInTreeMap);
TEST_DECLARE(TestInTreeMapInsert);
TEST_DECLARE(TestWriteTiming);
TEST_DECLARE(TestInTreeMapInsertRandom);

TEST_DEFINE(TestMain)
{
	TEST_RUN(
		//TestInTreeMap,
		//TestInTreeMapInsert,
		//TestInTreeMapInsertRandom,
		//TestStorage,
		TestStorageCapacity,
		//TestStorageBigTransaction,
		//TestStorageBigRandomTransaction,
		//TestWriteTiming,
	);
}
