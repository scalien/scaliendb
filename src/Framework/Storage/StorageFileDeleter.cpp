#include "StorageFileDeleter.h"
#include "System/FileSystem.h"
#include "System/Threading/Signal.h"

static volatile bool                    enabled = true;
static volatile bool                    running = false;
static ThreadPool*                      threadPool = NULL;
static Mutex                            enabledMutex;

/*
===============================================================================================

 StorageFileDeleterTask

===============================================================================================
*/

class StorageFileDeleterTask
{
public:
    StorageFileDeleterTask(const char* filename);

    void                        DeleteFile();

private:
    Buffer                      filename;
};


StorageFileDeleterTask::StorageFileDeleterTask(const char* filename_)
{
    filename.Write(filename_);
    filename.NullTerminate();
}

void StorageFileDeleterTask::DeleteFile()
{
    MutexGuard      enabledGuard(enabledMutex);

    if (running)
    {
        Log_Debug("Deleting %s", filename.GetBuffer());

        FS_Delete(filename.GetBuffer());
    }

    delete this;
}

/*
===============================================================================================

 StorageFileDeleter

===============================================================================================
*/

void StorageFileDeleter::Init()
{
    ASSERT(threadPool == NULL);

    running = true;
    threadPool = ThreadPool::Create(1);
    threadPool->Start();
}

void StorageFileDeleter::Shutdown()
{
    ASSERT(threadPool != NULL);

    running = false;
    if (!enabled)
        enabledMutex.Unlock();

    delete threadPool;
    threadPool = NULL;
}

void StorageFileDeleter::Delete(const char* filename)
{
    StorageFileDeleterTask*     task;

    // task will delete itself
    task = new StorageFileDeleterTask(filename);
    threadPool->Execute(MFUNC_OF(StorageFileDeleterTask, DeleteFile, task));
}

void StorageFileDeleter::SetEnabled(bool enabled_)
{
    if (!enabled && enabled_)
    {
        enabled = true;
        enabledMutex.Unlock();
    }
    else if (enabled && !enabled_)
    {
        enabled = false;
        // wait for pending deletes to complete
        enabledMutex.Lock();
    }
}

bool StorageFileDeleter::IsEnabled()
{
    return enabled;
}

Mutex& StorageFileDeleter::GetMutex()
{
    return enabledMutex;
}
