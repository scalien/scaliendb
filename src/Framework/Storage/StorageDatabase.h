#ifndef STORAGEDATABASE_H
#define STORAGEDATABASE_H

#include "StorageTable.h"

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
    
    StorageTable*           GetTable(const char* tableName);
    void                    CloseTable(const char* tableName);
    void                    Close();
    
    void                    Commit(bool recovery = true, bool flush = true);
    
private:
    Buffer                  name;
    Buffer                  path;
    InList<StorageTable>    tables;
    bool                    closed;
};

#endif
