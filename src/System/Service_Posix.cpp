#ifndef PLATFORM_WINDOWS
#include "Service.h"

bool Service::Main(int argc, char **argv, void (*serviceFunc)())
{
    UNUSED(argc);
    UNUSED(argv);

    serviceFunc();
    return true;
}

void Service::SetStatus(unsigned status)
{
    UNUSED(status);
}

#endif
