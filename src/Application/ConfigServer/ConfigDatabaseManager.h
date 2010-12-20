#ifndef CONFIGDATABASEMANAGER_H
#define CONFIGDATABASEMANAGER_H

#include "Framework/Storage/StorageEnvironment.h"
#include "Framework/Storage/StorageShardProxy.h"
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
    void                    Shutdown();
    
    ConfigState*            GetConfigState();
    
    void                    SetPaxosID(uint64_t paxosID);
    uint64_t                GetPaxosID();
    
    StorageShardProxy*      GetSystemShard();
    StorageShardProxy*      GetQuorumShard();
    
    void                    Read();
    void                    Write();

private:
    uint64_t                paxosID;
    ConfigState             configState;
    Buffer                  writeBuffer;
    StorageEnvironment      environment;
    StorageShardProxy       systemConfigShard;
    StorageShardProxy       quorumShard;
};

#endif
