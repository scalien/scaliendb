#ifndef CRASHREPORTER_H
#define CRASHREPORTER_H

#include "Events/Callable.h"

class CrashReporter
{
public:
    static void         SetCallback(Callable callback);
    static void         DeleteCallback();
    static const char*  GetReport();
    static void         ReportSystemEvent(const char* ident);

#ifdef DEBUG
    static void         TimedCrash(unsigned intervalMsec);
    static void         RandomCrash(unsigned intervalMsec);
#endif
};

#endif
