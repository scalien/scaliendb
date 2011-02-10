#ifndef IOPROCESSOR_H
#define IOPROCESSOR_H

#include "IOOperation.h"

#define IOPROCESSOR_NO_BLOCK            0
#define IOPROCESSOR_BLOCK_ALL           1
#define IOPROCESSOR_BLOCK_INTERACTIVE   2

class Callable; // forward

/*
===============================================================================================

 IOProcessor

===============================================================================================
*/

class IOProcessor
{
public:
    static bool Init(int maxfd);
    static void Shutdown();

    static bool Add(IOOperation* ioop);
    static bool Remove(IOOperation* ioop);

    static bool Poll(int sleep);

    static bool Complete(Callable* callable);

    static void BlockSignals(int blockMode);

    static bool IsRunning();
};

#endif
