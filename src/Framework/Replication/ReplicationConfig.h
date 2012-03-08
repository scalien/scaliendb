#ifndef REPLICATIONCONFIG_H
#define REPLICATIONCONFIG_H

#include "System/Common.h"
#include "Framework/Storage/StorageShardProxy.h"

#define REPLICATION_CONFIG (ReplicationConfig::Get())
#define MY_NODEID          (ReplicationConfig::Get()->GetNodeID())

/*
===============================================================================================

 ReplicationConfig

===============================================================================================
*/

class ReplicationConfig
{
    typedef StorageShardProxy ShardProxy;

private:
    ReplicationConfig();

public:
    static ReplicationConfig* Get();
    
    void        Init(StorageShardProxy* shard);
    void        Shutdown();
    void        SetNodeID(uint64_t nodeID);
    uint64_t    GetNodeID();
    void        SetRunID(uint64_t runID);
    uint64_t    GetRunID();
    void        SetClusterID(uint64_t clusterID);
    uint64_t    GetClusterID();
    uint64_t    NextProposalID(uint64_t proposalID);
    void        Commit();
    
private:
    ShardProxy* shard;
    uint64_t    nodeID;
    uint64_t    runID;
    uint64_t    clusterID;
};

#endif
