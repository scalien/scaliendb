#ifndef IOPROCESSOR_H
#define IOPROCESSOR_H

#include "IOOperation.h"

class Callable; // forward

/*
===============================================================================================

 IOProcessor

===============================================================================================
*/

class IOProcessor
{
public:
    static bool Init(int maxfd, bool blockSignals);
    static void Shutdown();

    static bool Add(IOOperation* ioop);
    static bool Remove(IOOperation* ioop);

    static bool Poll(int sleep);

    static bool Complete(Callable& callable);
};

#endif
