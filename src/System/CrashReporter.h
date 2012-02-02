#ifndef CRASHREPORTER_H
#define CRASHREPORTER_H

#include "Events/Callable.h"

class CrashReporter
{
public:
    static void         SetCallback(Callable callback);
    static const char*  GetReport();
};

#endif
