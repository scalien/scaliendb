#include "Test.h"
#include "System/Macros.h"

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
