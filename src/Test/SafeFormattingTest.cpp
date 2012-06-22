#include "Test.h"
#include "System/SafeFormatting.h"
#include "System/Buffers/ReadBuffer.h"

class SafeFormatting_UserType
{
public:
    operator ReadBuffer() { return ReadBuffer(); }
};

TEST_DEFINE(TestSafeFormattingBasic)
{
    char    buf[100];
    int     i;
    char    c;
    float   f;
    int*    pi;
    int     ret;

    SafeFormatting_UserType userType;

    i = 12345;
    ret = SFWritef(buf, sizeof(buf), "{0}", i);
    c = 'X';
    ret = SFWritef(buf, sizeof(buf), "{0}", c);
    f = 1.2;
    ret = SFWritef(buf, sizeof(buf), "{0}", f);
    pi = &i;
    ret = SFWritef(buf, sizeof(buf), "{0}", *pi);
    ret = SFWritef(buf, sizeof(buf), "{0}", "a");
    ret = SFWritef(buf, sizeof(buf), "{0}", userType);

    ret = SFWritef(buf, sizeof(buf), "{0} {1} {0}", i, c);

    return TEST_SUCCESS;
}
