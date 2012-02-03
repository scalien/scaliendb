#include "Test.h"
#include "System/CrashReporter.h"

#include <string>

void CrashReporterCallback()
{
    const char* msg;

    msg = CrashReporter::GetReport();
    Log_Message("%s", msg);

    _exit(TEST_SUCCESS);
}

TEST_DEFINE(TestCrashReporterInvalidAccess)
{
    char*   ptr;

    CrashReporter::SetCallback(CFunc(CrashReporterCallback));

    ptr = 0;
    ptr[0] = 0;

    return TEST_SUCCESS;
}

static void CorruptedStackAfterReturn()
{
    char    x[1];

    memset(x, 0, 1000);
}

TEST_DEFINE(TestCrashReporterStackOverflow)
{
    CrashReporter::SetCallback(CFunc(CrashReporterCallback));
    CorruptedStackAfterReturn();

    return TEST_SUCCESS;
}

TEST_DEFINE(TestCrashReporterNullPointerMemberFunction)
{
    std::string*    pString;

    CrashReporter::SetCallback(CFunc(CrashReporterCallback));

    pString = NULL;
    pString->append("x");

    return TEST_SUCCESS;
}

TEST_DEFINE(TestCrashReporterDivisionByZero)
{
    CrashReporter::SetCallback(CFunc(CrashReporterCallback));

    int d = 0;
    int a = 5/d;

    return TEST_SUCCESS;
}

TEST_DEFINE(TestCrashReporterDoubleFree)
{
    char*   p;
    
    CrashReporter::SetCallback(CFunc(CrashReporterCallback));

    p = new char[1];
    delete p;
    delete p;

    return TEST_SUCCESS;
}
