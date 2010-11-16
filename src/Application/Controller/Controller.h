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

#include "ConfigDatabaseManager.h"
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

    uint64_t                    GetNodeID();

    ConfigDatabaseManager*      GetDatabaseManager();
    ConfigQuorumProcessor*      GetQuorumProcessor();
    ConfigHeartbeatManager*     GetHeartbeatManager();
    ConfigActivationManager*    GetActivationManager();

    void                        OnConfigStateChanged();
    
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
    bool                        isCatchingUp;

    ConfigDatabaseManager       databaseManager;
    ConfigQuorumProcessor       quorumProcessor;
    ConfigHeartbeatManager      heartbeatManager;
    ConfigPrimaryLeaseManager   primaryLeaseManager;
    ConfigActivationManager     activationManager;
};

#endif
