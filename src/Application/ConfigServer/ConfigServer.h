#ifndef CONFIGSERVER_H
#define CONFIGSERVER_H

#include "Application/Common/ClusterContext.h"
#include "Application/SDBP/SDBPContext.h"
#include "ConfigDatabaseManager.h"
#include "ConfigQuorumProcessor.h"
#include "ConfigPrimaryLeaseManager.h"
#include "ConfigHeartbeatManager.h"
#include "ConfigActivationManager.h"
#include "ConfigSplitShardManager.h"

/*
===============================================================================================

 ConfigServer

===============================================================================================
*/

class ConfigServer : public ClusterContext, public SDBPContext
{
public:
    void                        Init();
    void                        Shutdown();

    uint64_t                    GetNodeID();

    ConfigDatabaseManager*      GetDatabaseManager();
    ConfigQuorumProcessor*      GetQuorumProcessor();
    ConfigHeartbeatManager*     GetHeartbeatManager();
    ConfigActivationManager*    GetActivationManager();
    ConfigSplitShardManager*    GetSplitShardManager();

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
    ConfigDatabaseManager       databaseManager;
    ConfigQuorumProcessor       quorumProcessor;
    ConfigHeartbeatManager      heartbeatManager;
    ConfigPrimaryLeaseManager   primaryLeaseManager;
    ConfigActivationManager     activationManager;
    ConfigSplitShardManager     splitShardManager;
};

#endif
