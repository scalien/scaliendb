#ifndef CONTROLLER_H
#define CONTROLLER_H

#include "ConfigMessage.h"
#include "ConfigQuorumContext.h"
#include "System/Containers/InList.h"
#include "System/Containers/InSortedList.h"
#include "System/Events/Timer.h"
#include "Framework/Storage/StorageDatabase.h"
#include "Framework/Storage/StorageEnvironment.h"
#include "Application/Common/ClusterContext.h"
#include "Application/Common/ClientRequest.h"
#include "Application/SDBP/SDBPContext.h"
#include "Application/ConfigState/ConfigState.h"
#include "Application/Common/CatchupMessage.h"

#include "ConfigQuorumProcessor.h"
#include "ConfigPrimaryLeaseManager.h"
#include "ConfigHeartbeatManager.h"
#include "ConfigActivationManager.h"

class ClientSession;            // forward

#define HEARTBEAT_EXPIRE_TIME           7000        // msec
#define ACTIVATION_FAILED_PENALTY_TIME  60*60*1000  // msec, 1 hour

/*
===============================================================================================

 Controller

===============================================================================================
*/

class Controller : public ClusterContext, public SDBPContext
{
public:
    void                        Init();
    void                        Shutdown();

    bool                        IsMaster();
    int64_t                     GetMaster();
    uint64_t                    GetNodeID();

    ConfigState*                GetConfigState();
    ConfigQuorumProcessor*      GetQuorumProcessor();
    ConfigHeartbeatManager*     GetHeartbeatManager();
    ConfigActivationManager*    GetActivationManager();

    void                        OnConfigStateChanged()
    {
        UpdateActivationTimeout();
        UpdateListeners();
    }

    // ========================================================================================
    // For ConfigContext:
    //
    void                        OnLeaseTimeout();
    void                        OnIsLeader();
    void                        OnAppend(uint64_t paxosID, ConfigMessage& message, bool ownAppend);
    void                        OnStartCatchup();
    void                        OnCatchupMessage(CatchupMessage& message);
    
    // ========================================================================================
    // SDBPContext interface:
    //
    bool                        IsValidClientRequest(ClientRequest* request);
    void                        OnClientRequest(ClientRequest* request);
    void                        OnClientClose(ClientSession* session);

    // ========================================================================================
    // ClusterContext interface:
    //
    void                        OnClusterMessage(uint64_t nodeID, ClusterMessage& msg);
    void                        OnIncomingConnectionReady(uint64_t nodeID, Endpoint endpoint);
    bool                        OnAwaitingNodeID(Endpoint endpoint);
    // ========================================================================================

private:
    void                        ToClientResponse(ConfigMessage* message, ClientResponse* response);
    void                        TryRegisterShardServer(Endpoint& endpoint);
    void                        ReadConfigState();
    void                        WriteConfigState();
    void                        SendClientResponse(ConfigMessage& message);
    void                        UpdatePrimaryLeaseTimer();
    void                        UpdateActivationTimeout();
    void                        UpdateListeners();
    
    uint64_t                    configStatePaxosID;
    bool                        isCatchingUp;
    ConfigState                 configState;
    Buffer                      configStateBuffer;
    StorageDatabase*            systemDatabase;
    StorageEnvironment          databaseEnv;

    ConfigQuorumProcessor       quorumProcessor;
    ConfigHeartbeatManager      heartbeatManager;
    ConfigActivationManager     activationManager;
};

#endif
