#ifndef CONTROLLER_H
#define CONTROLLER_H

#include "ConfigMessage.h"
#include "ConfigContext.h"
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

#define HEARTBEAT_EXPIRE_TIME       7000 //msec

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

    int64_t             GetMaster();
    uint64_t            GetNodeID();
    uint64_t            GetReplicationRound();
    ConfigState*        GetConfigState();
    void                RegisterHeartbeat(uint64_t nodeID);
    bool                HasHeartbeat(uint64_t nodeID);

    // ========================================================================================
    // For ConfigContext:
    //
    void                OnLearnLease();
    void                OnLeaseTimeout();
    void                OnAppend(ConfigMessage& message, bool ownAppend);
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
    void                TryAppend();
    void                FromClientRequest(ClientRequest* request, ConfigMessage* message);
    void                ToClientResponse(ConfigMessage* message, ClientResponse* response);
    void                OnPrimaryLeaseTimeout();
    void                OnHeartbeatTimeout();
    void                TryDeactivateShardServer(uint64_t nodeID);
    void                TryActivatingShardServer(uint64_t nodeID);
//    void                TryActivateShardServer(uint64_t nodeID);
    void                TryRegisterShardServer(Endpoint& endpoint);
    void                ReadConfigState();
    void                WriteConfigState();
    void                SendClientResponse(ConfigMessage& message);
    void                OnHeartbeat(ClusterMessage& message);
    void                OnRequestLease(ClusterMessage& message);
    void                AssignPrimaryLease(ConfigQuorum& quorum, ClusterMessage& message);
    void                ExtendPrimaryLease(ConfigQuorum& quorum, ClusterMessage& message);
    void                UpdatePrimaryLeaseTimer();
    void                UpdateListeners();
    
    uint64_t            configStatePaxosID;
    bool                isCatchingUp;
    ConfigContext       configContext;
    ConfigState         configState;
    MessageList         configMessages;
    LeaseList           primaryLeases;
    Timer               primaryLeaseTimeout;
    HeartbeatList       heartbeats;
    Countdown           heartbeatTimeout;
    RequestList         requests;
    RequestList         listenRequests;
    Buffer              configStateBuffer;
    StorageDatabase*    systemDatabase;
    StorageEnvironment  databaseEnv;
};

/*
===============================================================================================

 PrimaryLease

===============================================================================================
*/

class PrimaryLease
{
public:
    PrimaryLease()  { prev = next = this; }

    uint64_t        quorumID;
    uint64_t        nodeID;
    uint64_t        expireTime;
    
    PrimaryLease*   prev;
    PrimaryLease*   next;
};

inline bool LessThan(PrimaryLease &a, PrimaryLease &b)
{
    return (a.expireTime < b.expireTime);
}

/*
===============================================================================================

 Heartbeat

===============================================================================================
*/

class Heartbeat
{
public:

    Heartbeat()     { prev = next = this; }

    uint64_t        nodeID;
    uint64_t        expireTime;
    
    Heartbeat*      prev;
    Heartbeat*      next;
};

inline bool LessThan(Heartbeat &a, Heartbeat &b)
{
    return (a.expireTime < b.expireTime);
}

#endif
