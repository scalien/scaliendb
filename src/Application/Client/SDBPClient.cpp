#include "SDBPClient.h"
#include "SDBPControllerConnection.h"
#include "SDBPShardConnection.h"
#include "SDBPClientConsts.h"
#include "System/IO/IOProcessor.h"
#include "System/Mutex.h"
#include "Framework/Replication/PaxosLease/PaxosLease.h"
#include "Application/Common/ClientRequest.h"
#include "Application/Common/ClientResponse.h"

#define CLIENT_MULTITHREAD 
#ifdef CLIENT_MULTITHREAD

Mutex        clientMutex;

#define CLIENT_MUTEX_GUARD_DECLARE()    MutexGuard mutex(clientMutex)
#define CLIENT_MUTEX_LOCK()             mutex.Lock()
#define CLIENT_MUTEX_UNLOCK()           mutex.Unlock()

#else // CLIENT_MULTITHREAD

#define CLIENT_MUTEX_GUARD_DECLARE()
#define CLIENT_MUTEX_LOCK()
#define CLIENT_MUTEX_UNLOCK()

#endif // CLIENT_MULTITHREAD

#define MAX_IO_CONNECTION   1024

#define VALIDATE_CONFIG_STATE()     \
    if (configState == NULL)        \
    {                               \
        result->Close();            \
        EventLoop();                \
    }                               \
    if (configState == NULL)        \
        return SDBP_NOSERVICE;      \


#define VALIDATE_CONTROLLER()       \
    if (numControllers == 0)        \
        return SDBP_API_ERROR;


#define CLIENT_DATA_COMMAND(op, ...)                \
    Request*    req;                                \
                                                    \
    CLIENT_MUTEX_GUARD_DECLARE();                   \
                                                    \
    if (!isDatabaseSet || !isTableSet)              \
        return SDBP_BADSCHEMA;                      \
                                                    \
    req = new Request;                              \
    req->op(NextCommandID(), tableID, __VA_ARGS__); \
    requests.Append(req);                           \
                                                    \
    if (isBatched)                                  \
    {                                               \
        result->AppendRequest(req);                 \
        return SDBP_SUCCESS;                        \
    }                                               \
                                                    \
    result->Close();                                \
    result->AppendRequest(req);                     \
                                                    \
    CLIENT_MUTEX_UNLOCK();                          \
    EventLoop();                                    \
    return result->CommandStatus();                 \


#define CLIENT_SCHEMA_COMMAND(op, ...)              \
    Request*    req;                                \
                                                    \
    CLIENT_MUTEX_GUARD_DECLARE();                   \
    VALIDATE_CONTROLLER();                          \
                                                    \
    if (configState == NULL)                        \
    {                                               \
        result->Close();                            \
        CLIENT_MUTEX_UNLOCK();                      \
        EventLoop();                                \
        CLIENT_MUTEX_LOCK();                        \
    }                                               \
                                                    \
    if (configState == NULL)                        \
        return SDBP_NOSERVICE;                      \
                                                    \
    req = new Request;                              \
    req->op(NextCommandID(), __VA_ARGS__);          \
                                                    \
    requests.Append(req);                           \
                                                    \
    result->Close();                                \
    result->AppendRequest(req);                     \
                                                    \
    CLIENT_MUTEX_UNLOCK();                          \
    EventLoop();                                    \
    return result->CommandStatus();                 \


using namespace SDBPClient;

static uint64_t Hash(uint64_t h)
{
    return h;
}

static uint64_t Key(ShardConnection* conn)
{
    return conn->GetNodeID();
}

static int KeyCmp(uint64_t a, uint64_t b)
{
    if (a < b)
        return -1;
    if (a > b)
        return 1;
    return 0;
}

Client::Client()
{
    master = -1;
    commandID = 0;
    masterCommandID = 0;
    configState = NULL;
    isDatabaseSet = false;
    databaseID = 0;
    isTableSet = false;
    tableID = 0;
    numControllers = 0;
    globalTimeout.SetCallable(MFUNC(Client, OnGlobalTimeout));
    masterTimeout.SetCallable(MFUNC(Client, OnMasterTimeout));
    result = NULL;
}

Client::~Client()
{
    Shutdown();
}

int Client::Init(int nodec, const char* nodev[])
{
    CLIENT_MUTEX_GUARD_DECLARE();

    // sanity check on parameters
    if (nodec <= 0 || nodev == NULL)
        return SDBP_API_ERROR;

    // TODO: find out the optimal size of MAX_SERVER_NUM
    if (!IOProcessor::Init(MAX_IO_CONNECTION))
        return SDBP_API_ERROR;

    IOProcessor::BlockSignals(IOPROCESSOR_BLOCK_INTERACTIVE);

    // set default timeouts
    masterTimeout.SetDelay(3 * PAXOSLEASE_MAX_LEASE_TIME);
    globalTimeout.SetDelay(SDBP_DEFAULT_TIMEOUT);
    
    connectivityStatus = SDBP_NOCONNECTION;
    timeoutStatus = SDBP_SUCCESS;
    
    result = new Result;
    
    controllerConnections = new ControllerConnection*[nodec];
    for (int i = 0; i < nodec; i++)
    {
        Endpoint    endpoint;
        
        endpoint.Set(nodev[i], true);
        controllerConnections[i] = new ControllerConnection(this, (uint64_t) i, endpoint);
    }
    numControllers = nodec;
    
    master = -1;
    masterTime = 0;
    commandID = 0;
    masterCommandID = 0;
        
    isBatched = false;
    
    return SDBP_SUCCESS;
}

void Client::Shutdown()
{
    CLIENT_MUTEX_GUARD_DECLARE();

    if (!controllerConnections)
        return;
    
    for (int i = 0; i < numControllers; i++)
        delete controllerConnections[i];
    
    delete[] controllerConnections;
    controllerConnections = NULL;
    
    delete result;
    
    shardConnections.DeleteTree();
    
    EventLoop::Remove(&masterTimeout);
    EventLoop::Remove(&globalTimeout);
    IOProcessor::Shutdown();
}

void Client::SetGlobalTimeout(uint64_t timeout)
{
    CLIENT_MUTEX_GUARD_DECLARE();

    if (globalTimeout.IsActive())
    {
        EventLoop::Remove(&globalTimeout);
        globalTimeout.SetDelay(timeout);
        EventLoop::Add(&globalTimeout);
    }
    else
        globalTimeout.SetDelay(timeout);
}

void Client::SetMasterTimeout(uint64_t timeout)
{
    CLIENT_MUTEX_GUARD_DECLARE();
    
    if (masterTimeout.IsActive())
    {
        EventLoop::Remove(&masterTimeout);
        masterTimeout.SetDelay(timeout);
        EventLoop::Add(&masterTimeout);
    }
    else
        masterTimeout.SetDelay(timeout);
}

uint64_t Client::GetGlobalTimeout()
{
    return globalTimeout.GetDelay();
}

uint64_t Client::GetMasterTimeout()
{
    return masterTimeout.GetDelay();
}

Result* Client::GetResult()
{
    Result* tmp;
    
    tmp = result;
    result = new Result;
    return tmp;
}

int Client::TransportStatus()
{
    return result->TransportStatus();
}

int Client::ConnectivityStatus()
{
    return connectivityStatus;
}

int Client::TimeoutStatus()
{
    return timeoutStatus;
}

int Client::CommandStatus()
{
    return result->CommandStatus();
}

// return Command status
int Client::GetDatabaseID(ReadBuffer& name, uint64_t& databaseID)
{
    ConfigDatabase* database;

    CLIENT_MUTEX_GUARD_DECLARE();
    VALIDATE_CONTROLLER();
    
    if (configState == NULL)
    {
        result->Close();
        CLIENT_MUTEX_UNLOCK();
        EventLoop();
        CLIENT_MUTEX_LOCK();
    }
    
    if (configState == NULL)
        return SDBP_NOSERVICE;
    
    database = configState->GetDatabase(name);
    if (!database)
        return SDBP_BADSCHEMA;
    
    databaseID = database->databaseID;
    return SDBP_SUCCESS;
}

// return Command status
int Client::GetTableID(ReadBuffer& name, uint64_t databaseID, uint64_t& tableID)
{
    ConfigTable*    table;
    
    VALIDATE_CONTROLLER();
    
    assert(configState != NULL);
    table = configState->GetTable(databaseID, name);
    if (!table)
        return SDBP_BADSCHEMA;
    
    tableID = table->tableID;
    Log_Trace("%U", tableID);
    return SDBP_SUCCESS;
}

int Client::UseDatabase(ReadBuffer& name)
{
    int         ret;

    VALIDATE_CONTROLLER();
    
    isDatabaseSet = false;
    isTableSet = false;
    ret = GetDatabaseID(name, databaseID);
    if (ret != SDBP_SUCCESS)
        return ret;
    
    isDatabaseSet = true;
    return SDBP_SUCCESS;
}

int Client::UseTable(ReadBuffer& name)
{
    int         ret;

    VALIDATE_CONTROLLER();

    if (!isDatabaseSet)
        return SDBP_BADSCHEMA;

    isTableSet = false;
    ret = GetTableID(name, databaseID, tableID);
    if (ret != SDBP_SUCCESS)
        return ret;
    Log_Trace("%U", tableID);
    isTableSet = true;
    return SDBP_SUCCESS;
}

int Client::CreateQuorum(List<uint64_t>& nodes)
{
    CLIENT_SCHEMA_COMMAND(CreateQuorum, nodes);
}

int Client::CreateDatabase(ReadBuffer& name)
{
    CLIENT_SCHEMA_COMMAND(CreateDatabase, name);
}

int Client::RenameDatabase(uint64_t databaseID, const ReadBuffer& name)
{
    CLIENT_SCHEMA_COMMAND(RenameDatabase, databaseID, (ReadBuffer&) name);
}

int Client::DeleteDatabase(uint64_t databaseID)
{
    CLIENT_SCHEMA_COMMAND(DeleteDatabase, databaseID);
}

int Client::CreateTable(uint64_t databaseID, uint64_t quorumID, ReadBuffer& name)
{
    CLIENT_SCHEMA_COMMAND(CreateTable, databaseID, quorumID, name);
}

int Client::RenameTable(uint64_t databaseID, uint64_t tableID, ReadBuffer& name)
{
    CLIENT_SCHEMA_COMMAND(RenameTable, databaseID, tableID, name);
}

int Client::DeleteTable(uint64_t databaseID, uint64_t tableID)
{
    CLIENT_SCHEMA_COMMAND(DeleteTable, databaseID, tableID);
}

int Client::SplitShard(uint64_t shardID, ReadBuffer& splitKey)
{
    CLIENT_SCHEMA_COMMAND(SplitShard, shardID, splitKey);
}

int Client::Get(const ReadBuffer& key)
{
    CLIENT_DATA_COMMAND(Get, (ReadBuffer&) key);
}

int Client::Set(const ReadBuffer& key, const ReadBuffer& value)
{
    CLIENT_DATA_COMMAND(Set, (ReadBuffer&) key, (ReadBuffer&) value);
}

int Client::SetIfNotExists(ReadBuffer& key, ReadBuffer& value)
{
    CLIENT_DATA_COMMAND(SetIfNotExists, key, value);
}

int Client::TestAndSet(ReadBuffer& key, ReadBuffer& test, ReadBuffer& value)
{
    CLIENT_DATA_COMMAND(TestAndSet, key, test, value);
}

int Client::GetAndSet(ReadBuffer& key, ReadBuffer& value)
{
    CLIENT_DATA_COMMAND(GetAndSet, key, value);
}

int Client::Add(const ReadBuffer& key, int64_t number)
{
    CLIENT_DATA_COMMAND(Add, (ReadBuffer&) key, number);
}

int Client::Append(const ReadBuffer& key, const ReadBuffer& value)
{
    CLIENT_DATA_COMMAND(Append, (ReadBuffer&) key, (ReadBuffer&) value);
}

int Client::Delete(ReadBuffer& key)
{
    CLIENT_DATA_COMMAND(Delete, key);
}

int Client::Remove(ReadBuffer& key)
{
    CLIENT_DATA_COMMAND(Remove, key);
}

int Client::Begin()
{
    Log_Trace();

    CLIENT_MUTEX_GUARD_DECLARE();

    result->Close();
    isBatched = true;
    
    return SDBP_SUCCESS;
}

int Client::Submit()
{
    Log_Trace();

    EventLoop();
    isBatched = false;
    
    return result->TransportStatus();
}

int Client::Cancel()
{
    Log_Trace();

    CLIENT_MUTEX_GUARD_DECLARE();

    requests.Clear();

    result->Close();
    isBatched = false;
    
    return SDBP_SUCCESS;
}

bool Client::IsBatched()
{
    return isBatched;
}

void Client::EventLoop()
{
    if (!controllerConnections)
    {
        result->transportStatus = SDBP_API_ERROR;
        return;
    }
    
    CLIENT_MUTEX_GUARD_DECLARE();
    
    EventLoop::UpdateTime();

    Log_Trace("%U", databaseID);
    Log_Trace("%U", tableID);
    AssignRequestsToQuorums();
    SendQuorumRequests();
    
    EventLoop::UpdateTime();
    EventLoop::Reset(&globalTimeout);
    EventLoop::Reset(&masterTimeout);
    timeoutStatus = SDBP_SUCCESS;
    
    while (!IsDone())
    {
        EventLoop::RunTimers();
        if (IsDone())
            break;
        if (!IOProcessor::Poll(0))
            break;

        // let other threads to enter IOProcessor and complete requests
        CLIENT_MUTEX_UNLOCK();
        CLIENT_MUTEX_LOCK();
    }
        
    requests.Clear();
    
    result->connectivityStatus = connectivityStatus;
    result->timeoutStatus = timeoutStatus;
    result->Begin();
}

bool Client::IsDone()
{
    // TODO: configState???
    
    if (result->GetRequestCount() == 0 && configState != NULL)
        return true;
    
    if (result->TransportStatus() == SDBP_SUCCESS)
        return true;
    
    if (timeoutStatus != SDBP_SUCCESS)
        return true;
    
    return false;
}

uint64_t Client::NextCommandID()
{
    return ++commandID;
}

Request* Client::CreateGetConfigState()
{
    Request*    req;
    
    req = new Request;
    req->GetConfigState(NextCommandID());
    
    return  req;
}

void Client::SetMaster(int64_t master_, uint64_t nodeID)
{
    Log_Trace("known master: %d, set master: %d, nodeID: %d", (int) master, (int) master_, (int) nodeID);
    
    if (master_ == (int64_t) nodeID)
    {
        if (master != master_)
        {
            // node became the master
            Log_Message("Node %d is the master", nodeID);
            master = master_;
            connectivityStatus = SDBP_SUCCESS;
            
            // TODO: it is similar to ResendRequests
            //SendRequest(nodeID, safeRequests);
        }
        // else node is still the master
        EventLoop::Reset(&masterTimeout);
    }
    else if (master_ < 0 && master == (int64_t) nodeID)
    {
        // node lost its mastership
        Log_Message("Node %d lost its mastership", nodeID);
        master = -1;
        connectivityStatus = SDBP_NOMASTER;
        
        if (!IsSafe())
            return;

        // TODO: send safe requests that had no response to the new master
        // ResendRequests(nodeID);

        // TODO: What's this? -> set master timeout (copy-paste from Keyspace)
    }
}

void Client::UpdateConnectivityStatus()
{
    // TODO: check all connection's connect status
    // if there aren't any connected nodes, set the
    // connectivityStatus to NOCONNECTION
}

void Client::OnGlobalTimeout()
{
    Log_Trace();
    timeoutStatus = SDBP_GLOBAL_TIMEOUT;
}

void Client::OnMasterTimeout()
{
    Log_Trace();
    timeoutStatus = SDBP_MASTER_TIMEOUT;
}

bool Client::IsSafe()
{
    // TODO:
    return true;
}

void Client::SetConfigState(ControllerConnection* conn, ConfigState* configState_)
{
    if (master < 0 || (uint64_t) master == conn->GetNodeID())
        configState = configState_;
    else
        return;

    // we know the state of the system, so we can start sending requests
    if (configState)
    {
        ConnectShardServers();
        AssignRequestsToQuorums();
        SendQuorumRequests();
    }
}

void Client::ReassignRequest(Request* req)
{
    uint64_t        quorumID;
    ConfigQuorum*   quorum;
    ReadBuffer      key;
    
    if (!configState)
        return;
    
    if (req->IsControllerRequest())
    {
        if (master >= 0)
            controllerConnections[master]->Send(req);
        else
            requests.Append(req);

        return;
    }
    
    key.Wrap(req->key);
    if (!GetQuorumID(req->tableID, key, quorumID))
        ASSERT_FAIL();

    // reassign the request to the new quorum
    req->quorumID = quorumID;

    quorum = configState->GetQuorum(quorumID);
    if (IsSafe() && quorum->hasPrimary == false)
        requests.Append(req);
    else
        AddRequestToQuorum(req);
}

void Client::AssignRequestsToQuorums()
{
    Request*        it;
    Request*        next;
    RequestList     requestsCopy;
    
    if (requests.GetLength() == 0)
        return;
    
    //Log_Trace("%U", requests.First()->tableID);

    requestsCopy = requests;
    requests.ClearMembers();

    for (it = requestsCopy.First(); it != NULL; it = next)
    {
        //Log_Trace("%U", it->tableID);
        next = requestsCopy.Remove(it);
        //Log_Trace("%U", it->tableID);
        ReassignRequest(it);
    }
}

bool Client::GetQuorumID(uint64_t tableID, ReadBuffer& key, uint64_t& quorumID)
{
    ConfigTable*    table;
    ConfigShard*    shard;
    ReadBuffer      firstKey;
    ReadBuffer      lastKey;
    uint64_t*       it;
    
    assert(configState != NULL);
    table = configState->GetTable(tableID);
    //Log_Trace("%U", tableID);
    assert(table != NULL);
    for (it = table->shards.First(); it != NULL; it = table->shards.Next(it))
    {
        shard = configState->GetShard(*it);
        if (shard == NULL)
            continue;

        firstKey.Wrap(shard->firstKey);
        lastKey.Wrap(shard->lastKey);
        if (ReadBuffer::Cmp(key, firstKey) >= 0 && 
         (lastKey.GetLength() == 0 || ReadBuffer::Cmp(key, lastKey) < 0))
        {
            quorumID = shard->quorumID;
            return true;
        }
    }
    
    // not found
    return false;
}

void Client::AddRequestToQuorum(Request* req, bool end)
{
    RequestList*        qrequests;
    
    if (!quorumRequests.Get(req->quorumID, qrequests))
    {
        qrequests = new RequestList;
        quorumRequests.Set(req->quorumID, qrequests);
    }

    if (end)
        qrequests->Append(req);
    else
        qrequests->Prepend(req);
}   
    
void Client::SendQuorumRequest(ShardConnection* conn, uint64_t quorumID)
{
    RequestList*        qrequests;
    Request*            req;
    ConfigQuorum*       quorum;

    if (!quorumRequests.Get(quorumID, qrequests))
        return;
    
    if (!configState)
        return;

    quorum = configState->GetQuorum(quorumID);
    if (!quorum)
        ASSERT_FAIL();
    
    // TODO: distribute dirty
    if (!IsSafe() || (quorum->hasPrimary && quorum->primaryID == conn->GetNodeID()))
    {
        while (qrequests->GetLength() > 0)
        {   
            req = qrequests->First();
            qrequests->Remove(req);
            if (!conn->SendRequest(req))
            {
                conn->Flush();
                break;
            }
            if (qrequests->GetLength() == 0)
                conn->SendSubmit(quorumID);
        }

        if (qrequests->GetLength() == 0)
            conn->Flush();
    }
}

void Client::SendQuorumRequests()
{
    ShardConnection*    conn;
    uint64_t*           qit;
    RequestList*        qrequests;
    
    Log_Trace();
    
    for (conn = shardConnections.First(); conn != NULL; conn = shardConnections.Next(conn))
    {
        if (conn->IsWritePending())
            continue;

        SortedList<uint64_t>& quorums = conn->GetQuorumList();
        Log_Trace("quorums.length = %u", quorums.GetLength());
        for (qit = quorums.First(); qit != NULL; qit = quorums.Next(qit))
        {
            if (!quorumRequests.Get(*qit, qrequests))
                continue;

            Log_Trace("qrequests.length = %u", qrequests->GetLength());
            
            SendQuorumRequest(conn, *qit);
        }
    }
}

void Client::InvalidateQuorum(uint64_t quorumID)
{
    ConfigQuorum*       quorum;
    ShardConnection*    shardConn;
    uint64_t*           nit;
    
    quorum = configState->GetQuorum(quorumID);
    if (!quorum)
        ASSERT_FAIL();
        
    quorum->hasPrimary = false;
    
    // invalidate shard connections
    for (nit = quorum->activeNodes.First(); nit != NULL; nit = quorum->activeNodes.Next(nit))
    {
        shardConn = shardConnections.Get<uint64_t>(*nit);
        assert(shardConn != NULL);
        shardConn->ClearQuorumMembership(quorumID);
    }
    
    InvalidateQuorumRequests(quorumID);
}

void Client::InvalidateQuorumRequests(uint64_t quorumID)
{
    RequestList*        qrequests;

    qrequests = NULL;
    if (!quorumRequests.Get(quorumID, qrequests))
        ASSERT_FAIL();
    
    // push back requests to unselected requests' queue
    requests.PrependList(*qrequests);
}

void Client::ConnectShardServers()
{
    ConfigShardServer*          ssit;
    ConfigQuorum*               qit;
    InList<ConfigShardServer>*  shardServers;
    InList<ConfigQuorum>*       quorums;
    ShardConnection*            shardConn;
    uint64_t*                   nit;
    Endpoint                    endpoint;
    
    // TODO: removal of node
    shardServers = &configState->shardServers;
    for (ssit = shardServers->First(); ssit != NULL; ssit = shardServers->Next(ssit))
    {
        shardConn = shardConnections.Get<uint64_t>(ssit->nodeID);
        if (shardConn == NULL)
        {
            endpoint = ssit->endpoint;
            endpoint.SetPort(ssit->sdbpPort);
            shardConn = new ShardConnection(this, ssit->nodeID, endpoint);
            shardConnections.Insert(shardConn);
        }
        else
        {
            Log_Trace("ssit: %s, shardConn: %s", ssit->endpoint.ToString(), shardConn->GetEndpoint().ToString());
            // TODO: remove this hack when shardserver's endpoint will be sent correctly in configState
            endpoint = ssit->endpoint;
            endpoint.SetPort(ssit->sdbpPort);
            assert(endpoint == shardConn->GetEndpoint());
            shardConn->ClearQuorumMemberships();
        }
    }
    
    // assign quorums to ShardConnections
    quorums = &configState->quorums;
    for (qit = quorums->First(); qit != NULL; qit = quorums->Next(qit))
    {
        for (nit = qit->activeNodes.First(); nit != NULL; nit = qit->activeNodes.Next(nit))
        {
            shardConn = shardConnections.Get<uint64_t>(*nit);
            assert(shardConn != NULL);
            shardConn->SetQuorumMembership(qit->quorumID);
        }
        
        for (nit = qit->inactiveNodes.First(); nit != NULL; nit = qit->inactiveNodes.Next(nit))
        {
            shardConn = shardConnections.Get<uint64_t>(*nit);
            assert(shardConn != NULL);
            shardConn->SetQuorumMembership(qit->quorumID);
        }
    }
}

void Client::OnControllerConnected(ControllerConnection* conn)
{
    conn->SendGetConfigState();
    if (connectivityStatus == SDBP_NOCONNECTION)
        connectivityStatus = SDBP_NOMASTER;
}

void Client::OnControllerDisconnected(ControllerConnection* conn)
{
    if (master == (int64_t) conn->GetNodeID())
        SetMaster(-1, conn->GetNodeID());
}
