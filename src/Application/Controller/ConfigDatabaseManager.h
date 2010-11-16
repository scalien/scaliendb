#ifndef CONFIGDATABASEMANAGER_H
#define CONFIGDATABASEMANAGER_H

#include "Framework/Storage/StorageDatabase.h"
#include "Framework/Storage/StorageEnvironment.h"
#include "Application/ConfigState/ConfigState.h"

/*
===============================================================================================

 ConfigDatabaseManager

===============================================================================================
*/

class ConfigDatabaseManager
{
public:
    void                    Init();
    
    StorageDatabase*        GetDatabase();
    ConfigState*            GetConfigState();
    
    void                    Read();
    void                    Write();

private:
    uint64_t                paxosID;
    ConfigState             configState;
    Buffer                  writeBuffer;
    StorageDatabase*        database;
    StorageEnvironment      environment;
};

#endif
