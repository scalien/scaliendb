#include "StorageEnvironment.h"
#include "StorageDataCache.h"
#include "System/Macros.h"
#include "System/FileSystem.h"

StorageEnvironment::StorageEnvironment()
{
    manageCache = false;
    sync = true;
}

StorageEnvironment::~StorageEnvironment()
{
    Close();
}

void StorageEnvironment::InitCache(uint64_t cacheSize)
{
    DCACHE->Init(cacheSize);
    manageCache = true;
}

void StorageEnvironment::SetSync(bool sync_)
{
    sync = sync_;
}

bool StorageEnvironment::Open(const char *path_)
{
    char    sep;
    
    // try to create environment directory
    if (!FS_IsDirectory(path_))
    {
        if (!FS_CreateDir(path_))
        {
            STOP_FAIL(1, "Cannot create directory: %s", path_);
            return false;
        }
    }
    
    sep = FS_Separator();
    path.Append(path_);
    if (path.GetBuffer()[path.GetLength() - 1] != sep)
        path.Append(&sep, 1);
    path.NullTerminate();
    
    return true;
}

void StorageEnvironment::Close()
{
    StorageDatabase*    it;
    
    
    for (it = databases.First(); it != NULL; it = databases.Delete(it))
        it->Close();
    
    if (manageCache)
    {
        DCACHE->Shutdown();
        manageCache = false;
    }
}

uint64_t StorageEnvironment::GetSize()
{
    StorageDatabase*    it;
    uint64_t            size;
    
    size = 0;
    FOREACH (it, databases)
        size += it->GetSize();
    
    return size;
}

StorageDatabase* StorageEnvironment::GetDatabase(const char* dbName)
{
    StorageDatabase*   it;
    
    for (it = databases.First(); it != NULL; it = databases.Next(it))
    {
        if (MEMCMP(it->GetName(), strlen(it->GetName()), dbName, strlen(dbName)))
            return it;
    }
    
    it = new StorageDatabase();
    it->Open(path.GetBuffer(), dbName);
    it->environment = this;
    databases.Append(it);
    
    return it;
}
    
void StorageEnvironment::Commit(bool recovery, bool flush)
{
    StorageDatabase*    it;
    
    if (recovery)
    {
        FOREACH (it, databases)
            it->CommitPhase1();

        // TODO: if the OS supports write barriers, then this sync not necessary!
        if (sync && flush)
            FS_Sync();
    }
    
    FOREACH (it, databases)
        it->CommitPhase2();

    if (sync && flush)
        FS_Sync();
    
    if (recovery)
    {
        FOREACH (it, databases)
            it->CommitPhase3();
    }
    
    FOREACH (it, databases)
        it->CommitPhase4();
}
