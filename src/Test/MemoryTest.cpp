#include "Test.h"
#include "System/Common.h"
#include "System/Time.h"

TEST_DEFINE(TestMemoryOutOfMemoryError)
{
    size_t  allocSize = 1024*KiB;

    while (true)
    {
        void* p = malloc(allocSize);
        if (p == NULL)
        {
            MSleep(1000);
            if (allocSize > 16)
                allocSize /= 2;
        }
    }
}