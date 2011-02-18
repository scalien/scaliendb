#ifndef CONFIGQUORUMPROCESSOR_H
#define CONFIGQUORUMPROCESSOR_H

#include "Application/Common/ClusterMessage.h"
#include "Application/Common/ClientRequest.h"
#include "Application/Common/CatchupMessage.h"
#include "ConfigQuorumContext.h"

class ConfigServer; // forward

/*
===============================================================================================

 ConfigQuorumProcessor

===============================================================================================
*/

class ConfigQuorumProcessor
{
    typedef InList<ConfigMessage>       MessageList;
    typedef InList<ClientRequest>       RequestList;

public:
    void                    Init(ConfigServer* configServer, unsigned numConfigServer,
                             StorageShardProxy* quorumPaxosShard, StorageShardProxy* quorumLogShard);
    void                    Shutdown();

    bool                    IsMaster();
    int64_t                 GetMaster();
    uint64_t                GetQuorumID();
    uint64_t                GetPaxosID();

    void                    TryAppend();

    void                    OnClientRequest(ClientRequest* request);
    void                    OnClientClose(ClientSession* session);

    bool                    HasActivateMessage(uint64_t quorumID, uint64_t nodeID);
    bool                    HasDeactivateMessage(uint64_t quorumID);

    void                    ActivateNode(uint64_t quorumID, uint64_t nodeID);
    void                    DeactivateNode(uint64_t quorumID, uint64_t nodeID);
 
    void                    TryRegisterShardServer(Endpoint& endpoint);
    void                    TryShardSplitBegin(uint64_t shardID, ReadBuffer splitKey);
    void                    TryShardSplitComplete(uint64_t shardID);
    
    void                    OnShardMigrationComplete(ClusterMessage& message);
       
    void                    UpdateListeners();

    // ========================================================================================
    // For ConfigQuorumContext:
    //
    void                    OnLearnLease();
    void                    OnLeaseTimeout();
    void                    OnIsLeader();
    void                    OnAppend(uint64_t paxosID, ConfigMessage& message, bool ownAppend);
    void                    OnStartCatchup();
    void                    OnCatchupMessage(CatchupMessage& message);

private:
    void                    ConstructMessage(ClientRequest* request, ConfigMessage* message);
    void                    ConstructResponse(ConfigMessage* message, ClientResponse* response);
    void                    SendClientResponse(ConfigMessage& message);

    bool                    isCatchingUp;

    bool                    migrating;
    uint64_t                migrateShardID;
    uint64_t                migrateQuorumID;

    ConfigQuorumContext     quorumContext;
    ConfigServer*           configServer;
    
    MessageList             configMessages;
    RequestList             requests;
    RequestList             listenRequests;
};

#endif
