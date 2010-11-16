#include "Test.h"
#include "System/Common.h"
#include "System/Time.h"
#include "System/ThreadPool.h"
#include "System/Events/Callable.h"

TEST_DEFINE(TestStorageRandomGetSetDelete);

static unsigned long    crashSleepTime = 0;
static bool             crash = false;

static void CrashFunc()
{
    char*   null;
    
    crashSleepTime = RandomInt(1000, 10 * 1000);
    MSleep(crashSleepTime);

    if (crash)
    {
        null = NULL;
        null[0] = 0;
    }
}

TEST_DEFINE(TestCrashStorage)
{
    int         ret;
    ThreadPool* thread;
    
    crash = true;
    thread = ThreadPool::Create(1);
    thread->Start();
    thread->Execute(CFunc(CrashFunc));
    
    while (true)
    {
        ret = TestStorageRandomGetSetDelete();
        if (ret != TEST_SUCCESS)
            return ret;
    }
    
    crash = false;
    return TEST_SUCCESS;
}

