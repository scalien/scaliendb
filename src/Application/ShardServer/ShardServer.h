#ifndef SHARDSERVER_H
#define SHARDSERVER_H

#include "Application/ConfigState/ConfigState.h"
#include "Application/Common/ClusterContext.h"
#include "Application/SDBP/SDBPContext.h"
#include "ShardQuorumProcessor.h"
#include "ShardDatabaseAdapter.h"
#include "ShardHeartbeatManager.h"

/*
===============================================================================================

 ShardServer

===============================================================================================
*/

class ShardServer : public ClusterContext, public SDBPContext
{
public:
    typedef InList<ShardQuorumProcessor>            QuorumProcessorList;
    typedef ArrayList<uint64_t, CONFIG_MAX_NODES>   NodeList;

    void                    Init();
    void                    Shutdown();
    
    ShardQuorumProcessor*   GetQuorumProcessor(uint64_t quorumID);
    QuorumProcessorList&    GetQuorumProcessors();
    ShardDatabaseAdapter&   GetDatabaseAdapter();
    ConfigState&            GetConfigState();

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
    void                    OnIncomingConnectionReady(uint64_t nodeID, Endpoint endpoint);
    bool                    OnAwaitingNodeID(Endpoint endpoint);

    bool                    IsLeaseKnown(uint64_t quorumID);
    bool                    IsLeaseOwner(uint64_t quorumID);
    uint64_t                GetLeaseOwner(uint64_t quorumID);

private:
    void                    OnSetConfigState(ClusterMessage& message);
    void                    ConfigureQuorum(ConfigQuorum* configQuorum, bool active);

    ConfigState             configState;
    NodeList                controllers;
    QuorumProcessorList     quorumProcessors;
    ShardHeartbeatManager   heartbeatManager;
    ShardDatabaseAdapter    databaseAdapter;
};

#endif
