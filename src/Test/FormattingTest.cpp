#include "Test.h"
#include "System/Macros.h"
#include "System/Buffers/Buffer.h"
#include "System/IO/FD.h"

extern int UIntToBuffer(char* buf, size_t bufsize, unsigned value);

TEST_DEFINE(TestFormattingUnsigned)
{
#ifndef PLATFORM_WINDOWS
    unsigned        num = (unsigned) -1;
    char            ubuf[CS_INT_SIZE(unsigned)];
    char            usbuf[CS_INT_SIZE(unsigned)];
    int             ret;
    int             sret;

    memset(ubuf, 0x55, sizeof(ubuf));
    memset(usbuf, 0x55, sizeof(usbuf));

    for (unsigned i = 0; i < num; i++)
    {
        ret = UIntToBuffer(ubuf, sizeof(ubuf), i);
        sret = snprintf(usbuf, sizeof(usbuf), "%u", i);
        TEST_ASSERT(ret == sret);
        if (!MEMCMP(ubuf, ret, usbuf, sret))
            TEST_FAIL();
    }
#endif
    return TEST_SUCCESS;
}

TEST_DEFINE(TestFormattingPadding)
{
    Buffer  tmp;
    
    tmp.Writef("%04d", 1);
    TEST_ASSERT(tmp.Cmp("0001") == 0);

    tmp.Writef("% 4d", 1);
    TEST_ASSERT(tmp.Cmp("   1") == 0);
    
    return TEST_SUCCESS;
}

TEST_DEFINE(TestFormattingFD)
{
    Buffer  tmp;
    FD      fd;

    fd = INVALID_FD;
    tmp.Writef("%d", (int) fd);
    TEST_ASSERT(tmp.Cmp("-1") == 0);

    return TEST_SUCCESS;
}

TEST_DEFINE(TestFormattingOverflow)
{
    #pragma omp parallel for
    for (int i = 0; i < 1000; i++)
    {
        Buffer          tmp;
        Buffer          big;
        Buffer          small;

        const unsigned  TMP_SIZE = 8 * MB;
        const unsigned  BIG_SIZE = 16 * MB;

        tmp.Allocate(TMP_SIZE);
        tmp.SetLength(TMP_SIZE);
    
        big.Allocate(BIG_SIZE);
        big.SetLength(BIG_SIZE);

        small.Write("hello");

        tmp.Appendf("%#B:%#B", &big, &small);
    }

    return TEST_SUCCESS;
}

