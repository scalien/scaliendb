#include "Test.h"

TEST_DECLARE(TestStorage);
TEST_DECLARE(TestInTreeMap);

int TestMain()
{
	TEST_RUN(
		//TestStorage, 
		TestInTreeMap,
	);
}
