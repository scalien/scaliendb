#ifndef CONFIGSERVER_H
#define CONFIGSERVER_H

#include "System/Containers/HashMap.h"
#include "Application/Common/ClusterContext.h"
#include "Application/SDBP/SDBPContext.h"
#include "ConfigDatabaseManager.h"
#include "ConfigQuorumProcessor.h"
#include "ConfigPrimaryLeaseManager.h"
#include "ConfigHeartbeatManager.h"
#include "ConfigActivationManager.h"

#define BROADCAST_HTTP_ENDPOINT_DELAY        1000

class ConfigServerApp;

/*
===============================================================================================

 ConfigServer

===============================================================================================
*/

class ConfigServer : public ClusterContext, public SDBPContext
{
public:
    ConfigServer();

    void                        Init(ConfigServerApp* app);
    void                        Shutdown();

    uint64_t                    GetNodeID();

    ConfigDatabaseManager*      GetDatabaseManager();
    ConfigQuorumProcessor*      GetQuorumProcessor();
    ConfigHeartbeatManager*     GetHeartbeatManager();
    ConfigPrimaryLeaseManager*  GetPrimaryLeaseManager();
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
    void                        OnConnectionEnd(uint64_t nodeID, Endpoint endpoint);
    bool                        OnAwaitingNodeID(Endpoint endpoint);
    // ========================================================================================

    bool                        GetControllerHTTPEndpoint(uint64_t nodeID, Endpoint& endpoint);
    void                        GetHTTPEndpoint(Endpoint& endpoint);
    unsigned                    GetNumSDBPClients();
    void                        SetLogStatTimeout(uint64_t timeout);

private:
    void                        OnBroadcastHTTPEndpoint();

    ConfigDatabaseManager       databaseManager;
    ConfigQuorumProcessor       quorumProcessor;
    ConfigHeartbeatManager      heartbeatManager;
    ConfigPrimaryLeaseManager   primaryLeaseManager;
    ConfigActivationManager     activationManager;
    List<uint64_t>              configServers;
    HashMap<uint64_t, Endpoint> httpEndpoints;
    Endpoint                    httpEndpoint;
    Countdown                   broadcastHTTPEndpoint;
    ConfigServerApp*            configServerApp;
};

#endif
