#ifndef STORAGEFILEDELETER_H
#define STORAGEFILEDELETER_H

#include "System/Threading/JobProcessor.h"
#include "System/Threading/Mutex.h"
#include "System/Containers/InList.h"
#include "System/Buffers/Buffer.h"

/*
===============================================================================================

 StorageFileDeleter

===============================================================================================
*/

class StorageFileDeleter
{
public:
    static void                 Init();
    static void                 Shutdown();

    static void                 Delete(const char* filename);
    static void                 SetEnabled(bool enabled);
    static bool                 IsEnabled();

    static Mutex&               GetMutex();
};

#endif
