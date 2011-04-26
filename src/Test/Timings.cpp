#include "Test.h"
#include "System/Stopwatch.h"
#include "System/Log.h"
#include "System/FileSystem.h"
#include "System/Common.h"
#include "System/Threading/ThreadPool.h"

#include <fcntl.h>

TEST_DEFINE(TestTimingBasicWrite)
{
#ifndef PLATFORM_WINDOWS

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
    
    TEST_LOG("elapsed: %ld, num: %u, num/s: %f", (long) sw.Elapsed(), num, num / sw.Elapsed() * 1000.0);
#endif    
    return TEST_SUCCESS;
}

TEST_DEFINE(TestTimingFileSystemWrite)
{
    FD          fd;
    Stopwatch   sw;
    char        buf[64*1024];
    unsigned    num = 10*1000;
    const char  filename[] = "tmpfile";
    
    fd = FS_Open(filename, FS_WRITEONLY | FS_CREATE | FS_APPEND);
    
    sw.Restart();
    for (unsigned i = 0; i < num; i++)
    {
        FS_FileWrite(fd, buf, sizeof(buf));
        FS_Sync(fd);
    }
    sw.Stop();
    
    FS_FileClose(fd);
    FS_Delete(filename);
    
    TEST_LOG("With sync(): elapsed: %ld, %s/s", (long) sw.Elapsed(), HUMAN_BYTES((float)num * sizeof(buf) / sw.Elapsed() * 1000.0));

    fd = FS_Open(filename, FS_WRITEONLY | FS_CREATE | FS_APPEND);
    
    sw.Restart();
    for (unsigned i = 0; i < num; i++)
    {
        FS_FileWrite(fd, buf, sizeof(buf));
    }
    sw.Stop();
    
    FS_FileClose(fd);
    FS_Delete(filename);
    
    TEST_LOG("Without sync(): elapsed: %ld, %s/s", (long) sw.Elapsed(), HUMAN_BYTES((float)num * sizeof(buf) / sw.Elapsed() * 1000.0));

    
    return TEST_SUCCESS;
}

static FD           parallelFd1;
static FD           parallelFd2;
static const char*  parallelFilename1 = "tmpname1";
static const char*  parallelFilename2 = "tmpname2";

static void ParallelWrite1()
{
    char        buf[64*1024];
    unsigned    num = 10*1000;
    Stopwatch   sw;
    
    sw.Restart();
    for (unsigned i = 0; i < num; i++)
    {
        FS_FileWrite(parallelFd1, buf, sizeof(buf));
        FS_Sync(parallelFd1);
    }
    sw.Stop();
    
    FS_FileClose(parallelFd1);
    FS_Delete(parallelFilename1);
    
    TEST_LOG("P1 elapsed: %ld, %s/s", (long) sw.Elapsed(), HUMAN_BYTES((float)num * sizeof(buf) / sw.Elapsed() * 1000.0));
}

static void ParallelWrite2()
{
    char        buf[64*1024];
    unsigned    num = 10*1000;
    Stopwatch   sw;
    
    sw.Restart();
    for (unsigned i = 0; i < num; i++)
    {
        FS_FileWrite(parallelFd2, buf, sizeof(buf));
        //FS_Sync(parallelFd2);
    }
    sw.Stop();
    
    FS_FileClose(parallelFd2);
    FS_Delete(parallelFilename2);
    
    TEST_LOG("P2 elapsed: %ld, %s/s", (long) sw.Elapsed(), HUMAN_BYTES((float)num * sizeof(buf) / sw.Elapsed() * 1000.0));
}

/*
static void ParallelWrite3()
{
    char        buf[512*1024];
    unsigned    num = 10*1000;
    Stopwatch   sw;
    
    sw.Restart();
    for (unsigned i = 0; i < num; i++)
    {
        FS_FileWrite(parallelFd2, buf, sizeof(buf));
        FS_Sync(parallelFd2);
    }
    sw.Stop();
    
    FS_FileClose(parallelFd2);
    FS_Delete(parallelFilename2);
    
    TEST_LOG("P2 elapsed: %ld, %s/s", (long) sw.Elapsed(), HUMAN_BYTES((float)num * sizeof(buf) / sw.Elapsed() * 1000.0));
}
*/

TEST_DEFINE(TestTimingFileSystemParallelWrite)
{
    ThreadPool* thread1;
    ThreadPool* thread2;
    
    parallelFd1 = FS_Open(parallelFilename1, FS_WRITEONLY | FS_CREATE | FS_APPEND);
    parallelFd2 = FS_Open(parallelFilename2, FS_WRITEONLY | FS_CREATE | FS_APPEND);
    
    thread1 = ThreadPool::Create(1);
    thread2 = ThreadPool::Create(1);
    
    thread1->Execute(CFunc(ParallelWrite1));
    thread2->Execute(CFunc(ParallelWrite2));

    thread1->Start();
    thread2->Start();
        
    thread1->WaitStop();
    thread2->WaitStop();
    
    return TEST_SUCCESS;
}

TEST_DEFINE(TestTimingWrite)
{
#ifndef PLATFORM_WINDOWS
//    Stopwatch       sw;
//    unsigned        u;
//    int             fd;
//    char            buf[4096];
//    const char      filename[] = "/tmp/test-write.XXXXXX";
//    int             ret;
//    
//    const unsigned num = 100*1024;
//    
//    {
//        fd = open(filename, O_RDWR | O_CREAT);
//        
//        sw.Start();
//        for (u = 0; u < num; u++)
//        {
//            ret = write(fd, buf, sizeof(buf));
//            if (ret < 0)
//                TEST_FAIL();
//        }
//        sw.Stop();
//        
//        close(fd);
//        unlink(filename);
//        
//        TEST_LOG("write elapsed: %ld, num: %u, num/s: %f", sw.Elapsed(), num, num / sw.Elapsed() * 1000.0);
//    }
//    
//    {
//        fd = open(filename, O_RDWR | O_CREAT);
//        
//        struct iovec    vecbuf[num];
//        unsigned        i;
//        
//        sw.Reset();
//        for (u = 0; u < num / 1024; u++)
//        {
//            for (i = 0; i < 1024; i++)
//            {
//                vecbuf[i].iov_base = (void*) buf;
//                vecbuf[i].iov_len = sizeof(buf);
//            }
//            
//            sw.Start();
//            ret = writev(fd, vecbuf, 1024);
//            sw.Stop();
//            if (ret < 0)
//            {
//                Log_Errno();
//                TEST_FAIL();
//            }
//        }
//        
//        close(fd);
//        unlink(filename);
//        
//        TEST_LOG("writev elapsed: %ld, num: %u, num/s: %f", sw.Elapsed(), num, num / sw.Elapsed() * 1000.0);
//    }
#endif    
    return TEST_SUCCESS;
}

extern int UIntToBuffer(char* buf, size_t bufsize, unsigned value);

TEST_DEFINE(TestTimingSnprintf)
{
    Stopwatch       sw;
    unsigned        num = 10000000;
    char            buf[32];

    sw.Reset();
    sw.Start();
    for (unsigned i = 0; i < num; i++)
        snprintf(buf, sizeof(buf), "%u", i);
    sw.Stop();
    TEST_LOG("snprintf elapsed: %ld, num: %u, num/s: %f", (long) sw.Elapsed(), num, num / sw.Elapsed() * 1000.0);

    sw.Reset();
    sw.Start();
    for (unsigned i = 0; i < num; i++)
        UIntToBuffer(buf, sizeof(buf), i);
    sw.Stop();
    TEST_LOG("UIntToBuffer elapsed: %ld, num: %u, num/s: %f", (long) sw.Elapsed(), num, num / sw.Elapsed() * 1000.0);
    
    return TEST_SUCCESS;
}
