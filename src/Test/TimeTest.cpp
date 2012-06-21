#include "Test.h"

#include "System/StopWatch.h"
#include "System/Platform.h"
#include "System/Threading/ThreadPool.h"

#include <stdio.h>

TEST_DEFINE(TestClock)
{
    Stopwatch   sw;
    uint64_t    now;
    uint64_t    tmp;
    unsigned    num;
    
    num = 10 * 1000 * 1000;
    
    sw.Reset();
    sw.Start();
    for (unsigned u = 0; u < num; u++)
        now = NowClock();
    sw.Stop();
    printf("Elapsed without clock: %ld msec\n", (long) sw.Elapsed());
    
    StartClock();
    MSleep(100);
    
    now = NowClock();
    sw.Reset();
    sw.Start();
    for (unsigned u = 0; u < num; u++)
    {
        tmp = NowClock();
        if (tmp != now)
        {
            //printf("Clock changed from %" PRIu64 " to %" PRIu64 "\n", now, tmp);
            now = tmp;
        }
    }
    sw.Stop();
    printf("Elapsed with clock: %ld msec\n", (long) sw.Elapsed());
    
    StopClock();
    
    return 0;
}

static void TestTimeMultithreadedNow_ThreadFunc()
{
    uint64_t    now;

    now = Now();
    MSleep(30 * 1000);
    now = Now();
}

TEST_DEFINE(TestTimeMultithreadedNow)
{
    ThreadPool*     threads;

    StopClock();

    threads = ThreadPool::Create(1);

    threads->Execute(CFunc(TestTimeMultithreadedNow_ThreadFunc));

    threads->Start();


    
    threads->WaitStop();

    return TEST_SUCCESS;
}

