#ifndef SDBPCLIENT_H
#define SDBPCLIENT_H

#include "System/Platform.h"
#include "System/Buffers/ReadBuffer.h"
#include "System/Containers/List.h"
#include "System/Containers/InTreeMap.h"
#include "System/Containers/HashMap.h"
#include "System/Events/Countdown.h"
#include "Application/Controller/State/ConfigState.h"
#include "SDBPShardConnection.h"
#include "SDBPControllerConnection.h"
#include "SDBPResult.h"

namespace SDBPClient
{

class ControllerConnection;

/*
===============================================================================================

 SDBPClient::Client

===============================================================================================
*/

class Client
{
public:
    Client();
    ~Client();

    int                 Init(int nodec, const char* nodev[]);
    void                Shutdown();

    void                SetGlobalTimeout(uint64_t timeout);
    void                SetMasterTimeout(uint64_t timeout);
    uint64_t            GetGlobalTimeout();
    uint64_t            GetMasterTimeout();

    // connection state related commands
    int                 GetMaster();
    void                DistributeDirty(bool dd);
    
    Result*             GetResult();

    int                 TransportStatus();
    int                 ConnectivityStatus();
    int                 TimeoutStatus();
    int                 CommandStatus();
    
    int                 GetDatabaseID(ReadBuffer& name, uint64_t& databaseID);
    int                 GetTableID(ReadBuffer& name, uint64_t databaseID, uint64_t& table);
    
    int                 Get(uint64_t databaseID, uint64_t tableID, ReadBuffer& key);
    int                 Set(uint64_t databaseID, uint64_t tableID, 
                            ReadBuffer& key,
                            ReadBuffer& value);

    int                 Delete(uint64_t databaseID, uint64_t tableID, ReadBuffer& key);

private:
    typedef InList<Request> RequestList;
    typedef InTreeMap<ShardConnection> ShardConnectionMap;
    typedef HashMap<uint64_t, RequestList*> RequestListMap;
    friend class ControllerConnection;
    friend class ShardConnection;
    friend class Table;
    // Controller connections
    // ShardServer connections
    // ClusterState
    //  - DatabaseMap:  database
    //  - ShardMap: shard => nodeID
    //  - NodeMap:  nodeID => ClientConnection
    
    void                EventLoop();
    bool                IsDone();
    uint64_t            NextCommandID();
    Request*            CreateGetConfigState();
    void                SetMaster(int64_t master, uint64_t nodeID);
    void                UpdateConnectivityStatus();
    void                OnGlobalTimeout();
    void                OnMasterTimeout();
    bool                IsSafe();
    void                SetConfigState(ConfigState* configState);

    void                ReassignRequest(Request* req);
    void                AssignRequestsToQuorums();
    bool                GetQuorumID(uint64_t tableID, ReadBuffer& key, uint64_t& quorumID);
    void                AddRequestToQuorum(Request* req, bool end = true);
    void                SendQuorumRequests(ShardConnection* conn, uint64_t quorumID);
    void                InvalidateQuorum(uint64_t quorumID);
    void                InvalidateQuorumRequests(uint64_t quorumID);

    void                ConnectShardServers();
    ShardConnection*    GetShardConnection(uint64_t nodeID);

    void                OnControllerConnected(ControllerConnection* conn);
    void                OnControllerDisconnected(ControllerConnection* conn);
    
    int64_t                 master;
    uint64_t                commandID;
    uint64_t                masterCommandID;
    uint64_t                masterTime;
    int                     connectivityStatus;
    int                     timeoutStatus;
    Countdown               globalTimeout;
    Countdown               masterTimeout;
    ConfigState*            configState;
    Result*                 result;
    RequestList             requests;
    ShardConnectionMap      shardConnections;
    ControllerConnection**  controllerConnections;
    int                     numControllers;
    RequestListMap          quorumRequests;
};

};  // namespace

#endif