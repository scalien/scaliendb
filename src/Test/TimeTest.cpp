#include "Test.h"

#include "System/StopWatch.h"
#include "System/Platform.h"

#include <stdint.h>
#include <stdio.h>
#include <inttypes.h>

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
    printf("Elapsed without clock: %ld msec\n", sw.Elapsed());
    
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
    printf("Elapsed with clock: %ld msec\n", sw.Elapsed());
    
    StopClock();
    
    return 0;
}

TEST_MAIN(TestClock);
