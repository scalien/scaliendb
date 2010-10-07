#include "StorageEnvironment.h"
#include "System/Macros.h"
#include "System/FileSystem.h"

StorageEnvironment::~StorageEnvironment()
{
    Close();
}

bool StorageEnvironment::Open(const char *path_)
{
    char    sep;
    
    // try to create environment directory
    if (!FS_IsDirectory(path_))
    {
        if (!FS_CreateDir(path_))
            return false;
    }

    sep = FS_Separator();
    path.Append(path_);
    if (path.GetBuffer()[path.GetLength() - 1] != sep)
        path.Append(&sep, 1);
    
    return true;
}

void StorageEnvironment::Close()
{
    StorageDatabase*    it;
    
    for (it = databases.First(); it != NULL; it = databases.Delete(it))
        it->Close();
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
    
    FOREACH (it, databases)
        it->Commit(recovery, flush);
}
