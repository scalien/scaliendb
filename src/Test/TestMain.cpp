#include "Test.h"

TEST_DECLARE(TestStorage);
TEST_DECLARE(TestStorageCapacity);
TEST_DECLARE(TestInTreeMap);

TEST_DEFINE(TestMain)
{
	TEST_RUN(
		//TestInTreeMap,
		//TestStorage,
		TestStorageCapacity,
	);
}
