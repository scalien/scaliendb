#ifndef SHARDSERVER_H
#define SHARDSERVER_H

#include "Application/ConfigState/ConfigState.h"
#include "Application/Common/ClusterContext.h"
#include "Application/SDBP/SDBPContext.h"
#include "ShardQuorumProcessor.h"
#include "ShardDatabaseManager.h"
#include "ShardHeartbeatManager.h"
#include "ShardMigrationWriter.h"

/*
===============================================================================================

 ShardServer

===============================================================================================
*/

class ShardServer : public ClusterContext, public SDBPContext
{
public:
    typedef InList<ShardQuorumProcessor> QuorumProcessorList;

    void                    Init();
    void                    Shutdown();

    ShardQuorumProcessor*   GetQuorumProcessor(uint64_t quorumID);
    QuorumProcessorList*    GetQuorumProcessors();
    ShardDatabaseManager*   GetDatabaseManager();
    ConfigState*            GetConfigState();

    void                    BroadcastToControllers(Message& message);
    
    bool                    IsSendingShard();
    uint64_t                GetShardMigrationShardID();
    uint64_t                GetShardMigrationQuorumID();
    uint64_t                GetShardMigrationNodeID();
    uint64_t                GetShardMigrationBytesSent();
    uint64_t                GetShardMigrationBytesTotal();
    uint64_t                GetShardMigrationThroughput();

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
    void                    OnIncomingConnectionReady(uint64_t nodeID, Endpoint endpoint);
    bool                    OnAwaitingNodeID(Endpoint endpoint);
    // ========================================================================================

    bool                    IsLeaseKnown(uint64_t quorumID);
    bool                    IsLeaseOwner(uint64_t quorumID);
    uint64_t                GetLeaseOwner(uint64_t quorumID);

    unsigned                GetHTTPPort();
    unsigned                GetSDBPPort();

private:
    void                    OnSetConfigState(ClusterMessage& message);
    void                    ConfigureQuorum(ConfigQuorum* configQuorum);

    ConfigState             configState;
    List<uint64_t>          configServers;
    QuorumProcessorList     quorumProcessors;
    ShardHeartbeatManager   heartbeatManager;
    ShardDatabaseManager    databaseManager;
    ShardMigrationWriter    migrationWriter;
};

#endif
