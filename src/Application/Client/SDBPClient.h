#ifndef SDBPCLIENT_H
#define SDBPCLIENT_H

#include "System/Platform.h"
#include "System/Buffers/ReadBuffer.h"
#include "System/Containers/List.h"
#include "System/Containers/InTreeMap.h"
#include "System/Containers/HashMap.h"
#include "System/Events/Countdown.h"
#include "System/Mutex.h"
#include "Application/Common/ClientRequest.h"
#include "Application/ConfigState/ConfigState.h"
#include "SDBPShardConnection.h"
#include "SDBPControllerConnection.h"
#include "SDBPResult.h"
#include "SDBPClientConsts.h"

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

    int                     Init(int nodec, const char* nodev[]);
    void                    Shutdown();

    // timeout
    void                    SetGlobalTimeout(uint64_t timeout);
    void                    SetMasterTimeout(uint64_t timeout);
    uint64_t                GetGlobalTimeout();
    uint64_t                GetMasterTimeout();
    
    uint64_t                GetCurrentDatabaseID();
    uint64_t                GetCurrentTableID();
    
    void                    SetBatchLimit(uint64_t batchLimit);
    void                    SetBulkLoading(bool bulk);
    bool                    IsBulkLoading();
    
    // result
    Result*                 GetResult();

    // status
    int                     TransportStatus();
    int                     ConnectivityStatus();
    int                     TimeoutStatus();
    int                     CommandStatus();
    
    // config state
    
    // controller commands
    int                     CreateQuorum(List<uint64_t>& nodes);
    int                     DeleteQuorum(uint64_t quourumID);
    int                     AddNode(uint64_t quorumID, uint64_t nodeID);
    int                     RemoveNode(uint64_t quorumID, uint64_t nodeID);
    int                     ActivateNode(uint64_t nodeID);

    int                     CreateDatabase(ReadBuffer& name);
    int                     RenameDatabase(uint64_t databaseID, const ReadBuffer& name);
    int                     DeleteDatabase(uint64_t databaseID);

    int                     CreateTable(uint64_t databaseID, uint64_t quorumID, ReadBuffer& name);
    int                     RenameTable(uint64_t tableID, ReadBuffer& name);
    int                     DeleteTable(uint64_t tableID);
    int                     TruncateTable(uint64_t tableID);

    int                     SplitShard(uint64_t shardID, ReadBuffer& splitKey);
    
    // shard server commands
    int                     GetDatabaseID(ReadBuffer& name, uint64_t& databaseID);
    int                     GetTableID(ReadBuffer& name, uint64_t databaseID, uint64_t& tableID);
    int                     UseDatabase(ReadBuffer& name);
    int                     UseTable(ReadBuffer& name);
    
    int                     Get(const ReadBuffer& key);
    int                     Set(const ReadBuffer& key, const ReadBuffer& value);
    int                     SetIfNotExists(const ReadBuffer& key, const ReadBuffer& value);
    int                     TestAndSet(const ReadBuffer& key, const ReadBuffer& test, const ReadBuffer& value);
    int                     GetAndSet(const ReadBuffer& key, const ReadBuffer& value);
    int                     Add(const ReadBuffer& key, int64_t number);
    int                     Append(const ReadBuffer& key, const ReadBuffer& value);
    int                     Delete(const ReadBuffer& key);
    int                     Remove(const ReadBuffer& key);

    int                     ListKeys(const ReadBuffer& startKey, unsigned count, unsigned offset);
    int                     ListKeyValues(const ReadBuffer& startKey, unsigned count, unsigned offset);    
    int                     Count(const ReadBuffer& startKey, unsigned count, unsigned offset);    

    int                     Filter(const ReadBuffer& startKey, unsigned count, unsigned offset, uint64_t& commandID);
    int                     Receive(uint64_t commandID);
    
    int                     Begin();
    int                     Submit();
    int                     Cancel();
    bool                    IsBatched();

private:
    typedef InList<Request>                 RequestList;
    typedef InTreeMap<ShardConnection>      ShardConnectionMap;
    typedef HashMap<uint64_t, RequestList*> RequestListMap;

    friend class            ControllerConnection;
    friend class            ShardConnection;
    friend class            Table;
    
    void                    EventLoop();
    bool                    IsDone();
    uint64_t                NextCommandID();
    Request*                CreateGetConfigState();
    void                    SetMaster(int64_t master, uint64_t nodeID);
    void                    UpdateConnectivityStatus();
    void                    OnGlobalTimeout();
    void                    OnMasterTimeout();
    bool                    IsSafe();
    void                    SetConfigState(ControllerConnection* conn, ConfigState* configState);

    void                    ReassignRequest(Request* req);
    void                    AssignRequestsToQuorums();
    bool                    GetQuorumID(uint64_t tableID, ReadBuffer& key, uint64_t& quorumID);
    void                    AddRequestToQuorum(Request* req, bool end = true);
    void                    SendQuorumRequest(ShardConnection* conn, uint64_t quorumID);
    void                    SendQuorumRequests();
    void                    ClearQuorumRequests();
    void                    InvalidateQuorum(uint64_t quorumID, uint64_t nodeID);
    void                    InvalidateQuorumRequests(uint64_t quorumID);

    void                    ConfigureShardServers();
    ShardConnection*        GetShardConnection(uint64_t nodeID);

    void                    OnControllerConnected(ControllerConnection* conn);
    void                    OnControllerDisconnected(ControllerConnection* conn);
    
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
    bool                    isDatabaseSet;
    uint64_t                databaseID;
    bool                    isTableSet;
    uint64_t                tableID;
    bool                    isBatched;
    uint64_t                batchLimit;
    bool                    isBulkLoading;

//#ifdef CLIENT_MULTITHREAD    
    Mutex                   mutex;
    void                    Lock();
    void                    Unlock();
    void                    LockGlobal();
    void                    UnlockGlobal();
    bool                    IsGlobalLocked();
//#endif
};

};  // namespace

#endif