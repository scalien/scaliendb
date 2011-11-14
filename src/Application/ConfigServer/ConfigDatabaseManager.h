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
    StorageShardProxy*      GetQuorumPaxosShard();
    StorageShardProxy*      GetQuorumLogShard();
    
    bool                    ShardExists(uint64_t tableID, ReadBuffer firstKey);
    
    void                    Read();
    void                    Write();

    void                    SetControllers();

private:
    uint64_t                paxosID;
    ConfigState             configState;
    Buffer                  writeBuffer;
    StorageEnvironment      environment;
    StorageShardProxy       systemConfigShard;
    StorageShardProxy       quorumPaxosShard;
    StorageShardProxy       quorumLogShard;
};

#endif
