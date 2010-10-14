#ifndef STORAGEENVIRONMENT_H
#define STORAGEENVIRONMENT_H

#include "StorageDatabase.h"

/*
===============================================================================

 StorageEnvironment

===============================================================================
*/

class StorageEnvironment
{
public:
    StorageEnvironment();
    ~StorageEnvironment();

    void                    InitCache(uint64_t size);
    
    bool                    Open(const char *path);
    void                    Close();
    
    uint64_t                GetSize();
    StorageDatabase*        GetDatabase(const char* dbName);
    
    void                    Commit(bool recovery = true, bool flush = true);
    
private:
    Buffer                  name;
    Buffer                  path;
    InList<StorageDatabase> databases;
    bool                    manageCache;
};

#endif
