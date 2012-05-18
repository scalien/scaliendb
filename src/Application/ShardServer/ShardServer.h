#ifndef SHARDSERVER_H
#define SHARDSERVER_H

#include "Application/ConfigState/ConfigState.h"
#include "Application/Common/ClusterContext.h"
#include "Application/SDBP/SDBPContext.h"
#include "ShardQuorumProcessor.h"
#include "ShardDatabaseManager.h"
#include "ShardHeartbeatManager.h"
#include "ShardLockManager.h"
#include "ShardMigrationWriter.h"

class ShardServerApp;

/*
===============================================================================================

 ShardServer

===============================================================================================
*/

class ShardServer : public ClusterContext, public SDBPContext
{
public:
    typedef InList<ShardQuorumProcessor> QuorumProcessorList;

    void                    Init(ShardServerApp* app, bool restoreMode);
    void                    Shutdown();

    ShardQuorumProcessor*   GetQuorumProcessor(uint64_t quorumID);
    QuorumProcessorList*    GetQuorumProcessors();
    ShardDatabaseManager*   GetDatabaseManager();
    ShardLockManager*       GetLockManager();
    ShardMigrationWriter*   GetShardMigrationWriter();
    ShardHeartbeatManager*  GetHeartbeatManager();
    ConfigState*            GetConfigState();
    ShardServerApp*         GetShardServerApp();

    void                    BroadcastToControllers(Message& message);
    
    // ========================================================================================
    // SDBPContext interface:
    //
    bool                    IsValidClientRequest(ClientRequest* request);
    void                    OnClientRequest(ClientRequest* request);
    void                    OnClientClose(ClientSession* session);

    // ========================================================================================
    // ClusterContext interface:
    //
    void                    OnClusterMessage(uint64_t nodeID, ClusterMessage& msg);
    void                    OnConnectionReady(uint64_t nodeID, Endpoint endpoint);
    void                    OnConnectionEnd(uint64_t nodeID, Endpoint endpoint);
    bool                    OnAwaitingNodeID(Endpoint endpoint);
    // ========================================================================================

    bool                    IsLeaseKnown(uint64_t quorumID);
    bool                    IsLeaseOwner(uint64_t quorumID);
    uint64_t                GetLeaseOwner(uint64_t quorumID);

    unsigned                GetHTTPPort();
    unsigned                GetSDBPPort();
    void                    GetMemoryUsageBuffer(Buffer& buffer);
    unsigned                GetNumSDBPClients();
    uint64_t                GetStartTimestamp();

private:
    void                    OnSetConfigState(uint64_t nodeID, ClusterMessage& message);
    void                    ResetChangedConnections();
    void                    TryDeleteQuorumProcessors();
    void                    TryDeleteQuorumProcessor(ShardQuorumProcessor* quorumProcessor);
    void                    ConfigureQuorums();
    void                    ConfigureQuorum(ConfigQuorum* configQuorum);
    void                    TryDeleteShards();

    ConfigState             configState;
    List<uint64_t>          configServers;
    QuorumProcessorList     quorumProcessors;
    ShardHeartbeatManager   heartbeatManager;
    ShardDatabaseManager    databaseManager;
    ShardLockManager        lockManager;
    ShardMigrationWriter    migrationWriter;
    ShardServerApp*         shardServerApp;
    uint64_t                startTimestamp;
};

#endif
