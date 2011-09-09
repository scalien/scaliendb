#ifndef SDBPCLIENT_H
#define SDBPCLIENT_H

#include "System/Platform.h"
#include "System/Buffers/ReadBuffer.h"
#include "System/Containers/List.h"
#include "System/Containers/InTreeMap.h"
#include "System/Containers/HashMap.h"
#include "System/Events/Countdown.h"
#include "System/Threading/Mutex.h"
#include "System/Threading/Signal.h"
#include "Application/Common/ClientRequest.h"
#include "Application/ConfigState/ConfigState.h"
#include "SDBPShardConnection.h"
#include "SDBPControllerConnection.h"
#include "SDBPController.h"
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
    void                    OnClientShutdown();

    // =============================================================================================
    //
    // settings
    //
    void                    SetConsistencyMode(int consistencyMode);
    void					SetBatchMode(int batchMode);
    void                    SetBatchLimit(unsigned batchLimit);

    void                    SetGlobalTimeout(uint64_t timeout);
    void                    SetMasterTimeout(uint64_t timeout);
    uint64_t                GetGlobalTimeout();
    uint64_t                GetMasterTimeout();

    ConfigState*            GetConfigState();
    void                    WaitConfigState();
        
    // =============================================================================================
    //
    // result & status
    //
    Result*                 GetResult();

    int                     GetTransportStatus();
    int                     GetConnectivityStatus();
    int                     GetTimeoutStatus();
    int                     GetCommandStatus();
    
    // =============================================================================================
    //
    // controller commands
    //
    int                     CreateDatabase(ReadBuffer& name);
    int                     RenameDatabase(uint64_t databaseID, const ReadBuffer& name);
    int                     DeleteDatabase(uint64_t databaseID);

    int                     CreateTable(uint64_t databaseID, uint64_t quorumID, ReadBuffer& name);
    int                     RenameTable(uint64_t tableID, ReadBuffer& name);
    int                     DeleteTable(uint64_t tableID);
    int                     TruncateTable(uint64_t tableID);

    unsigned                GetNumQuorums();
    uint64_t                GetQuorumIDAt(unsigned n);
    void                    GetQuorumNameAt(unsigned n, Buffer& name);

    unsigned                GetNumDatabases();
    uint64_t                GetDatabaseIDAt(unsigned n);
    void                    GetDatabaseNameAt(unsigned n, Buffer& name);

    unsigned                GetNumTables(uint64_t databaseID);
    uint64_t                GetTableIDAt(uint64_t databaseID, unsigned n);
    void                    GetTableNameAt(uint64_t databaseID, unsigned n, Buffer& name);
    
    // =============================================================================================
    //
    // shard server commands
    //    
    int                     Get(uint64_t tableID, const ReadBuffer& key);
    int                     Set(uint64_t tableID, const ReadBuffer& key, const ReadBuffer& value);
    int                     Add(uint64_t tableID, const ReadBuffer& key, int64_t number);
    int                     Delete(uint64_t tableID, const ReadBuffer& key);

    int                     ListKeys(uint64_t tableID, const ReadBuffer& startKey, const ReadBuffer& endKey,
                             const ReadBuffer& prefix, unsigned count, bool skip);
    int                     ListKeyValues(uint64_t tableID, const ReadBuffer& startKey, const ReadBuffer& endKey, 
                             const ReadBuffer& prefix, unsigned count, bool skip);
    int                     Count(uint64_t tableID, const ReadBuffer& startKey, const ReadBuffer& endKey, 
                             const ReadBuffer& prefix);    

    int                     Filter(uint64_t tableID, const ReadBuffer& startKey, const ReadBuffer& endKey,
                             const ReadBuffer& prefix, unsigned count, uint64_t& commandID);
    int                     Receive(uint64_t commandID);
    
    int                     Begin();
    int                     Submit();
    int                     Cancel();

    Client*                 next;
    Client*                 prev;

private:
    typedef InList<Request>                     RequestList;
    typedef InTreeMap<Request>                  RequestMap;
    typedef InTreeMap<ShardConnection>          ShardConnectionMap;
    typedef HashMap<uint64_t, RequestList*>     RequestListMap;

    friend class            Controller;
    friend class            ShardConnection;
    friend class            Table;
    
    void                    ClearRequests();
    void                    EventLoop();
    bool                    IsDone();
    uint64_t                NextCommandID();
    Request*                CreateGetConfigState();
    int64_t                 GetMaster();
    void                    SetMaster(int64_t master);
    void                    UpdateConnectivityStatus();
    void                    OnGlobalTimeout();
    void                    OnMasterTimeout();
    void                    SetConfigState(ConfigState* configState);

    void                    AppendDataRequest(Request* req);
    void                    ReassignRequest(Request* req);
    void                    AssignRequestsToQuorums();
    bool                    GetQuorumID(uint64_t tableID, ReadBuffer& key, uint64_t& quorumID);
    void                    AddRequestToQuorum(Request* req, bool end = true);
    void                    SendQuorumRequest(ShardConnection* conn, uint64_t quorumID);
    void                    SendQuorumRequests();
    void                    ClearQuorumRequests();
    void                    InvalidateQuorum(uint64_t quorumID, uint64_t nodeID);
    void                    InvalidateQuorumRequests(uint64_t quorumID);
    void                    NextRequest(Request* req, ReadBuffer nextShardKey, ReadBuffer endKey,
                             ReadBuffer prefix, uint64_t count);

    void                    ConfigureShardServers();
    ShardConnection*        GetShardConnection(uint64_t nodeID);

    void                    OnControllerConnected(ControllerConnection* conn);
    void                    OnControllerDisconnected(ControllerConnection* conn);
    
    unsigned                GetMaxQuorumRequests(RequestList* qrequests, ShardConnection* conn, 
                             ConfigQuorum* quorum);
    uint64_t                GetRequestPaxosID();
    
    void                    ComputeListResponse();
    uint64_t                NumProxiedDeletes(Request* request);
    void                    TryWake();
    void                    IOThreadFunc();
    
    int64_t                 master;
    uint64_t                commandID;
    int                     connectivityStatus;
    int                     timeoutStatus;
    Countdown               globalTimeout;
    Countdown               masterTimeout;
    ConfigState*            configState;
    ConfigState             configStateCopy;
    Result*                 result;
    RequestList             requests;
    RequestMap              proxiedRequests;
    unsigned                batchLimit;
    int64_t                 proxySize;
    ShardConnectionMap      shardConnections;
    Controller*             controller;
    RequestListMap          quorumRequests;
    int                     consistencyMode;
    int						batchMode;
    uint64_t                highestSeenPaxosID;
    YieldTimer              onClientShutdown;

//#ifdef CLIENT_MULTITHREAD
    Signal                  isDone;
    Signal                  isShutdown;
    Mutex                   mutex;
    Buffer                  mutexName;
    void                    Lock();
    void                    Unlock();
    void                    LockGlobal();
    void                    UnlockGlobal();
    bool                    IsGlobalLocked();
    static Mutex&           GetGlobalMutex();
//#endif
};

};  // namespace

#endif
