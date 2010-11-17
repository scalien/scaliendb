#include "Test.h"
#include "System/Common.h"
#include "System/Time.h"
#include "System/ThreadPool.h"
#include "System/Events/Callable.h"

TEST_DEFINE(TestStorageRandomGetSetDelete);

static bool             crash = false;

unsigned RandomTimeout()
{
    return (unsigned)(1000.0 / RandomInt(1, 100) * 1000);
}

static void CrashFunc()
{
    char*   null;
    
    MSleep(RandomTimeout());

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
    
    SeedRandom();
    for (unsigned i = 0; i < 100; i++)
        TEST_LOG("%u", RandomTimeout());
    
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

