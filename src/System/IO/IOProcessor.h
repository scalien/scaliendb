#ifndef IOPROCESSOR_H
#define IOPROCESSOR_H

#include "IOOperation.h"

#define IOPROCESSOR_NO_BLOCK            0
#define IOPROCESSOR_BLOCK_ALL           1
#define IOPROCESSOR_BLOCK_INTERACTIVE   2

class Callable; // forward

/*
===============================================================================================

 IOProcessorStat

===============================================================================================
*/

class IOProcessorStat
{
public:
    uint64_t    numPolls;
    uint64_t    numTCPReads;
    uint64_t    numTCPWrites;
    uint64_t    numTCPBytesSent;
    uint64_t    numTCPBytesReceived;
    uint64_t    numUDPReads;
    uint64_t    numUDPWrites;
    uint64_t    numUDPBytesSent;
    uint64_t    numUDPBytesReceived;
    uint64_t    numCompletions;
    uint64_t    lastPollTime;
    uint64_t    totalPollTime;
    unsigned    lastNumEvents;
    uint64_t    totalNumEvents;
};

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

    static void Lock();
    static void Unlock();

    static bool Add(IOOperation* ioop);
    static bool Remove(IOOperation* ioop);

    static bool Poll(int sleep);

    static bool Complete(Callable* callable);

    static void BlockSignals(int blockMode);

    static bool IsRunning();
    
    static void GetStats(IOProcessorStat* stat);
};

#endif
