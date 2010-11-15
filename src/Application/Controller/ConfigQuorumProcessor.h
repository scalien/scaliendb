#ifndef CONFIGQUORUMPROCESSOR_H
#define CONFIGQUORUMPROCESSOR_H

#include "Application/Common/ClusterMessage.h"
#include "Application/Common/ClientRequest.h"
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
    uint64_t                GetQuorumID();
    uint64_t                GetPaxosID();

    void                    TryAppend();

    void                    OnClientRequest(ClientRequest* request);

    bool                    HasActivateMessage(uint64_t quorumID, uint64_t nodeID);
    bool                    HasDeactivateMessage(uint64_t quorumID, uint64_t nodeID);

    void                    ActivateNode(uint64_t quorumID, uint64_t nodeID);
    void                    DeactivateNode(uint64_t quorumID, uint64_t nodeID);

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

    ConfigQuorumContext     quorumContext;
    Controller*             controller;
    
    MessageList             configMessages;
    RequestList             requests;
    RequestList             listenRequests;
};

#endif
