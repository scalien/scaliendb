#include "Test.h"

TEST_DECLARE(TestStorage);
TEST_DECLARE(TestStorageCapacity);
TEST_DECLARE(TestInTreeMap);

int TestMain()
{
	TEST_RUN(
		//TestInTreeMap,
		//TestStorage,
		TestStorageCapacity,
	);
}
