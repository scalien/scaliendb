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

class ClientSession;            // forward
class PrimaryLease;             // forward
class Heartbeat;                // forward
class ConfigHeartbeatManager;       // forward
class ConfigQuorumProcessor;        // forward
class ConfigActivationManager;      // forward

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
    typedef InList<ConfigMessage>       MessageList;
    typedef InSortedList<PrimaryLease>  LeaseList;
    typedef InList<ClientRequest>       RequestList;
    typedef InSortedList<Heartbeat>     HeartbeatList;

    void                Init();
    void                Shutdown();

    bool                IsMaster();
    int64_t             GetMaster();
    uint64_t            GetNodeID();

    ConfigState*        GetConfigState();
    
    ConfigHeartbeatManager*     GetHeartbeatManager();
    ConfigQuorumProcessor*      GetQuorumProcessor();
    ConfigActivationManager*    GetActivationManager();

    void                OnConfigStateChanged()
    {
        UpdateActivationTimeout();
        UpdateListeners();
    }

//    void                RegisterHeartbeat(uint64_t nodeID);
//    bool                HasHeartbeat(uint64_t nodeID);

    // ========================================================================================
    // For ConfigContext:
    //
    void                OnLearnLease();
    void                OnLeaseTimeout();
    void                OnIsLeader();
    void                OnAppend(uint64_t paxosID, ConfigMessage& message, bool ownAppend);
    void                OnStartCatchup();
    void                OnCatchupMessage(CatchupMessage& message);
    
    // ========================================================================================
    // SDBPContext interface:
    //
    bool                IsValidClientRequest(ClientRequest* request);
    void                OnClientRequest(ClientRequest* request);
    void                OnClientClose(ClientSession* session);

    // ========================================================================================
    // ClusterContext interface:
    //
    void                OnClusterMessage(uint64_t nodeID, ClusterMessage& msg);
    void                OnIncomingConnectionReady(uint64_t nodeID, Endpoint endpoint);
    bool                OnAwaitingNodeID(Endpoint endpoint);
    // ========================================================================================

private:
    void                ToClientResponse(ConfigMessage* message, ClientResponse* response);
    void                TryRegisterShardServer(Endpoint& endpoint);
    void                ReadConfigState();
    void                WriteConfigState();
    void                SendClientResponse(ConfigMessage& message);
    void                UpdatePrimaryLeaseTimer();
    void                UpdateActivationTimeout();
    void                UpdateListeners();
    
    uint64_t            configStatePaxosID;
    bool                isCatchingUp;
    ConfigContext       configContext;
    ConfigState         configState;
    MessageList         configMessages;
    LeaseList           primaryLeases;
    Timer               primaryLeaseTimeout;
    Timer               activationTimeout;
    HeartbeatList       heartbeats;
    Countdown           heartbeatTimeout;
    RequestList         requests;
    RequestList         listenRequests;
    Buffer              configStateBuffer;
    StorageDatabase*    systemDatabase;
    StorageEnvironment  databaseEnv;
};

#endif
