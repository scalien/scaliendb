#include "Test.h"
#include "System/Log.h"
#include "System/Threading/ThreadPool.h"
#include "System/Events/Callable.h"

static void LogFiller()
{
    const char  filler[] = "01234567890123456789012345678901234567890123456789";

    while (true)
    {
        Log_Message("%s", filler);
    }
}

TEST_DEFINE(TestLogRotate)
{
    Log_SetOutputFile("test.log", true);
    Log_SetTarget(LOG_TARGET_STDOUT | LOG_TARGET_FILE);
    Log_SetMaxFileSize(100);
    
    LogFiller();    // never returns

    return TEST_SUCCESS;
}

TEST_DEFINE(TestLogRotateMultiThreaded)
{
    ThreadPool*     threadPool;
    unsigned        num = 100;

    Log_SetOutputFile("test.log", true);
    Log_SetTarget(LOG_TARGET_FILE);
    Log_SetMaxFileSize(100);

    threadPool = ThreadPool::Create(num);
    for (unsigned i = 0; i < num; i++)
    {
        threadPool->Execute(CFunc(LogFiller));
    }
    
    threadPool->Start();
    threadPool->WaitStop();

    return TEST_SUCCESS;    
}
