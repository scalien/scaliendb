#include "Test.h"
#include "System/Stopwatch.h"
#include "System/Log.h"

#include <fcntl.h>
#include <unistd.h>
#include <sys/uio.h>

TEST_DEFINE(TestBasicWriteTiming)
{
    Stopwatch       sw;
    unsigned        u;
    unsigned        num;
    int             fd;
    char            buf[4096];
    
    num = 10*1000*1000;
    
    fd = open("/dev/null", O_RDWR);
    
    sw.Start();
    for (u = 0; u < num; u++)
        write(fd, buf, sizeof(buf));    
    sw.Stop();
    
    TEST_LOG("elapsed: %ld, num: %u, num/s: %f", sw.Elapsed(), num, num / sw.Elapsed() * 1000.0);
    
    return TEST_SUCCESS;
}

TEST_DEFINE(TestWriteTiming)
{
    Stopwatch       sw;
    unsigned        u;
    unsigned        num;
    int             fd;
    char            buf[4096];
    const char      filename[] = "/tmp/test-write.XXXXXX";
    int             ret;
    
    num = 100*1024;
    
    {
        fd = open(filename, O_RDWR | O_CREAT);
        
        sw.Start();
        for (u = 0; u < num; u++)
        {
            ret = write(fd, buf, sizeof(buf));
            if (ret < 0)
                TEST_FAIL();
        }
        sw.Stop();
        
        close(fd);
        unlink(filename);
        
        TEST_LOG("write elapsed: %ld, num: %u, num/s: %f", sw.Elapsed(), num, num / sw.Elapsed() * 1000.0);
    }
    
    {
        fd = open(filename, O_RDWR | O_CREAT);
        
        struct iovec    vecbuf[num];
        unsigned        i;
        
        sw.Reset();
        for (u = 0; u < num / 1024; u++)
        {
            for (i = 0; i < 1024; i++)
            {
                vecbuf[i].iov_base = (void*) buf;
                vecbuf[i].iov_len = sizeof(buf);
            }
            
            sw.Start();
            ret = writev(fd, vecbuf, 1024);
            sw.Stop();
            if (ret < 0)
            {
                Log_Errno();
                TEST_FAIL();
            }
        }
        
        close(fd);
        unlink(filename);
        
        TEST_LOG("writev elapsed: %ld, num: %u, num/s: %f", sw.Elapsed(), num, num / sw.Elapsed() * 1000.0);
    }
    
    return TEST_SUCCESS;
}

extern int UIntToBuffer(char* buf, size_t bufsize, unsigned value);

TEST_DEFINE(TestSnprintfTiming)
{
    Stopwatch       sw;
    unsigned        num = 10000000;
    char            buf[32];

    sw.Reset();
    sw.Start();
    for (unsigned i = 0; i < num; i++)
        snprintf(buf, sizeof(buf), "%u", i);
    sw.Stop();
    TEST_LOG("snprintf elapsed: %ld, num: %u, num/s: %f", sw.Elapsed(), num, num / sw.Elapsed() * 1000.0);

    sw.Reset();
    sw.Start();
    for (unsigned i = 0; i < num; i++)
        UIntToBuffer(buf, sizeof(buf), i);
    sw.Stop();
    TEST_LOG("UIntToBuffer elapsed: %ld, num: %u, num/s: %f", sw.Elapsed(), num, num / sw.Elapsed() * 1000.0);
    
    return TEST_SUCCESS;
}
