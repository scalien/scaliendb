#ifndef PLATFORM_WINDOWS
#include "CrashReporter.h"

const char* CrashReporter::GetReport()
{
    // TODO:
    return "";
}

void CrashReporter::SetCallback(Callable callback)
{
    // TODO:
    UNUSED(callback);
}

void CrashReporter::DeleteCallback()
{
    // TODO:
}

#ifdef DEBUG
// Crash testing functionality support. Should NOT be included in release versions!
void CrashReporter::TimedCrash(unsigned intervalMsec)
{
    UNUSED(intervalMsec);
}

void CrashReporter::RandomCrash(unsigned intervalMsec)
{
    UNUSED(intervalMsec);
}
#endif // DEBUG

#endif
