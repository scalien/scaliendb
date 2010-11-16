#ifndef CONFIGQUORUMPROCESSOR_H
#define CONFIGQUORUMPROCESSOR_H

#include "Application/Common/ClusterMessage.h"
#include "Application/Common/ClientRequest.h"
#include "Application/Common/CatchupMessage.h"
#include "ConfigQuorumContext.h"

class Controller; // forward

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
    void                    Init(Controller* controller,
                             unsigned numControllers,  StorageTable* quorumTable);

    bool                    IsMaster();
    int64_t                 GetMaster();
    uint64_t                GetQuorumID();
    uint64_t                GetPaxosID();

    void                    TryAppend();

    void                    OnClientRequest(ClientRequest* request);
    void                    OnClientClose(ClientSession* session);

    bool                    HasActivateMessage(uint64_t quorumID, uint64_t nodeID);
    bool                    HasDeactivateMessage(uint64_t quorumID, uint64_t nodeID);

    void                    ActivateNode(uint64_t quorumID, uint64_t nodeID);
    void                    DeactivateNode(uint64_t quorumID, uint64_t nodeID);
 
    void                    TryRegisterShardServer(Endpoint& endpoint);
       
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
    void                    TransformRequest(ClientRequest* request, ConfigMessage* message);
    void                    TransfromMessage(ConfigMessage* message, ClientResponse* response);
    void                    SendClientResponse(ConfigMessage& message);

    ConfigQuorumContext     quorumContext;
    Controller*             controller;
    
    MessageList             configMessages;
    RequestList             requests;
    RequestList             listenRequests;
};

#endif
