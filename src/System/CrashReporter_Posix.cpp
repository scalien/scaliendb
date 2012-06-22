#ifndef PLATFORM_WINDOWS
#include "CrashReporter.h"

void CrashReporter::SetCallback(Callable callback)
{
    // TODO:
    UNUSED(callback);
}

void CrashReporter::DeleteCallback()
{
    // TODO:
}

const char* CrashReporter::GetReport()
{
    // TODO:
    return "";
}

void CrashReporter::ReportSystemEvent(const char* ident)
{
    // TODO:
    UNUSED(ident);
}

// Crash testing functionality support. Should NOT be included in release versions!
void CrashReporter::TimedCrash(unsigned intervalMsec)
{
    UNUSED(intervalMsec);
}

void CrashReporter::RandomCrash(unsigned intervalMsec)
{
    UNUSED(intervalMsec);
}

#endif
