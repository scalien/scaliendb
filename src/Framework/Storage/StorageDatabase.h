#ifndef STORAGEDATABASE_H
#define STORAGEDATABASE_H

#include "StorageTable.h"

class StorageEnvironment;   // forward

/*
===============================================================================

 StorageDatabase

===============================================================================
*/

class StorageDatabase
{
public:
    ~StorageDatabase();
    
    void                    Open(const char *path, const char* dbName);
    
    const char*             GetName();
    StorageEnvironment*     GetEnvironment();
    
    StorageTable*           GetTable(const char* tableName);
    void                    CloseTable(const char* tableName);
    void                    Close();
    
    void                    Commit(bool recovery = true, bool flush = true);
    
    StorageDatabase*        next;
    StorageDatabase*        prev;
    
private:
    Buffer                  name;
    Buffer                  path;
    InList<StorageTable>    tables;
    StorageEnvironment*     environment;
    
    friend class StorageEnvironment;
};

#endif
