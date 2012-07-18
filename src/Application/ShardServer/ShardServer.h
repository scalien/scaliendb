#ifndef SHARDSERVER_H
#define SHARDSERVER_H

#include "Application/ConfigState/ConfigState.h"
#include "Application/Common/ClusterContext.h"
#include "Application/SDBP/SDBPContext.h"
#include "ShardQuorumProcessor.h"
#include "ShardDatabaseManager.h"
#include "ShardHeartbeatManager.h"
#include "ShardTransactionManager.h"
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

    void                    Init(ShardServerApp* app, bool restoreMode, bool setNodeID, uint64_t myNodeID);
    void                    Shutdown();

    ShardQuorumProcessor*   GetQuorumProcessor(uint64_t quorumID);
    QuorumProcessorList*    GetQuorumProcessors();
    ShardDatabaseManager*   GetDatabaseManager();
    ShardMigrationWriter*   GetShardMigrationWriter();
    ShardHeartbeatManager*  GetHeartbeatManager();
    ShardTransactionManager* GetTransactionManager();
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
    void                    GetMemoryUsageBuffer(Buffer& buffer, ByteFormatType = BYTE_FORMAT_HUMAN);
    unsigned                GetNumSDBPClients();
    uint64_t                GetStartTimestamp();
    uint64_t                GetNumRequests();

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
    ShardTransactionManager transactionManager;
    ShardMigrationWriter    migrationWriter;
    ShardServerApp*         shardServerApp;
    uint64_t                startTimestamp;
    uint64_t                numRequests;
};

#endif
