#include "SDBPClient.h"
#include "SDBPControllerConnection.h"
#include "SDBPShardConnection.h"
#include "SDBPClientConsts.h"
#include "System/IO/IOProcessor.h"
#include "System/Mutex.h"
#include "System/ThreadPool.h"
#include "Framework/Replication/PaxosLease/PaxosLease.h"
#include "Application/Common/ClientRequest.h"
#include "Application/Common/ClientResponse.h"

#define MAX_IO_CONNECTION               1024
#define DEFAULT_BATCH_LIMIT             (100*MB)

#define CLIENT_MULTITHREAD 
#ifdef CLIENT_MULTITHREAD

// globalMutex protects the underlying single threaded IO and Event handling layer
Mutex   globalMutex;                    

#define CLIENT_MUTEX_GUARD_DECLARE()    MutexGuard mutexGuard(mutex)
#define CLIENT_MUTEX_GUARD_LOCK()       mutexGuard.Lock()
#define CLIENT_MUTEX_GUARD_UNLOCK()     mutexGuard.Unlock()

#define CLIENT_MUTEX_LOCK()             Lock()
#define CLIENT_MUTEX_UNLOCK()           Unlock()

#define GLOBAL_MUTEX_GUARD_DECLARE()    MutexGuard mutexGuard(globalMutex)
#define GLOBAL_MUTEX_GUARD_LOCK()       mutexGuard.Lock()
#define GLOBAL_MUTEX_GUARD_UNLOCK()     mutexGuard.Unlock()

#define YIELD()                         ThreadPool::YieldThread()

#else // CLIENT_MULTITHREAD

#define CLIENT_MUTEX_GUARD_DECLARE()
#define CLIENT_MUTEX_GUARD_LOCK()
#define CLIENT_MUTEX_GUARD_UNLOCK()

#define CLIENT_MUTEX_LOCK()
#define CLIENT_MUTEX_UNLOCK()

#define GLOBAL_MUTEX_GUARD_DECLARE()
#define GLOBAL_MUTEX_GUARD_LOCK()
#define GLOBAL_MUTEX_GUARD_UNLOCK()

#define YIELD()

#endif // CLIENT_MULTITHREAD

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
    int         status;                             \
                                                    \
    CLIENT_MUTEX_GUARD_DECLARE();                   \
                                                    \
    if (!isDatabaseSet || !isTableSet)              \
        return SDBP_BADSCHEMA;                      \
                                                    \
    req = new Request;                              \
    req->op(NextCommandID(), tableID, __VA_ARGS__); \
    if (AppendDataRequest(req, status))             \
        return status;                              \
                                                    \
    CLIENT_MUTEX_GUARD_UNLOCK();                    \
    EventLoop();                                    \
    return result->GetCommandStatus();              \


#define CLIENT_SCHEMA_COMMAND(op, ...)              \
    Request*    req;                                \
                                                    \
    CLIENT_MUTEX_GUARD_DECLARE();                   \
    VALIDATE_CONTROLLER();                          \
                                                    \
    if (configState == NULL)                        \
    {                                               \
        result->Close();                            \
        CLIENT_MUTEX_GUARD_UNLOCK();                \
        EventLoop();                                \
        CLIENT_MUTEX_GUARD_LOCK();                  \
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
    CLIENT_MUTEX_GUARD_UNLOCK();                    \
    EventLoop();                                    \
    return result->GetCommandStatus();              \


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
    batchLimit = DEFAULT_BATCH_LIMIT;
    isBulkLoading = false;

    globalMutex.SetName("ClientGlobalMutex");
    mutexName.Writef("Client_%p", this);
    mutexName.NullTerminate();
    mutex.SetName(mutexName.GetBuffer());
}

Client::~Client()
{
    Shutdown();
}

int Client::Init(int nodec, const char* nodev[])
{
    GLOBAL_MUTEX_GUARD_DECLARE();

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
    consistencyLevel = SDBP_CONSISTENCY_STRICT;
    highestSeenPaxosID = 0;
        
    isBatched = false;
    
    return SDBP_SUCCESS;
}

void Client::Shutdown()
{
    RequestListMap::Node*   requestNode;
    RequestList*            requestList;
//    Request*                request;
    
    GLOBAL_MUTEX_GUARD_DECLARE();

    if (!controllerConnections)
        return;
    
    for (int i = 0; i < numControllers; i++)
        delete controllerConnections[i];
    
    delete[] controllerConnections;
    controllerConnections = NULL;
        
    shardConnections.DeleteTree();

    ClearQuorumRequests();
    FOREACH (requestNode, quorumRequests)
    {
        requestList = requestNode->Value();
        delete requestList;
    }

    delete result;
    
    EventLoop::Remove(&masterTimeout);
    EventLoop::Remove(&globalTimeout);
    IOProcessor::Shutdown();
}

void Client::SetGlobalTimeout(uint64_t timeout)
{
    GLOBAL_MUTEX_GUARD_DECLARE();

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
    GLOBAL_MUTEX_GUARD_DECLARE();
    
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

uint64_t Client::GetCurrentDatabaseID()
{
    return databaseID;
}

uint64_t Client::GetCurrentTableID()
{
    return tableID;
}

void Client::SetBatchLimit(uint64_t batchLimit_)
{
    batchLimit = batchLimit_;
}

void Client::SetBulkLoading(bool bulk)
{
    isBulkLoading = bulk;
}

bool Client::IsBulkLoading()
{
    return isBulkLoading;
}

void Client::SetConsistencyLevel(int level)
{
    consistencyLevel = level;
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
    return result->GetTransportStatus();
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
    return result->GetCommandStatus();
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
    
    CLIENT_MUTEX_GUARD_DECLARE();
    VALIDATE_CONTROLLER();
    
    ASSERT(configState != NULL);
    table = configState->GetTable(databaseID, name);
    if (!table)
        return SDBP_BADSCHEMA;
    
    tableID = table->tableID;
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

    isTableSet = true;
    return SDBP_SUCCESS;
}

int Client::CreateQuorum(List<uint64_t>& nodes)
{
    CLIENT_SCHEMA_COMMAND(CreateQuorum, nodes);
}

int Client::DeleteQuorum(uint64_t quorumID)
{
    CLIENT_SCHEMA_COMMAND(DeleteQuorum, quorumID);
}

int Client::AddNode(uint64_t quorumID, uint64_t nodeID)
{
    CLIENT_SCHEMA_COMMAND(AddNode, quorumID, nodeID);
}

int Client::RemoveNode(uint64_t quorumID, uint64_t nodeID)
{
    CLIENT_SCHEMA_COMMAND(RemoveNode, quorumID, nodeID);
}

int Client::ActivateNode(uint64_t nodeID)
{
    CLIENT_SCHEMA_COMMAND(ActivateNode, nodeID);
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

int Client::RenameTable(uint64_t tableID, ReadBuffer& name)
{
    CLIENT_SCHEMA_COMMAND(RenameTable, tableID, name);
}

int Client::DeleteTable(uint64_t tableID)
{
    CLIENT_SCHEMA_COMMAND(DeleteTable, tableID);
}

int Client::TruncateTable(uint64_t tableID)
{
    CLIENT_SCHEMA_COMMAND(TruncateTable, tableID);
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

int Client::SetIfNotExists(const ReadBuffer& key, const ReadBuffer& value)
{
    CLIENT_DATA_COMMAND(SetIfNotExists, (ReadBuffer&) key, (ReadBuffer&) value);
}

int Client::TestAndSet(const ReadBuffer& key, const ReadBuffer& test, const ReadBuffer& value)
{
    CLIENT_DATA_COMMAND(TestAndSet, (ReadBuffer&) key, (ReadBuffer&) test, (ReadBuffer&) value);
}

int Client::GetAndSet(const ReadBuffer& key, const ReadBuffer& value)
{
    CLIENT_DATA_COMMAND(GetAndSet, (ReadBuffer&) key, (ReadBuffer&) value);
}

int Client::Add(const ReadBuffer& key, int64_t number)
{
    CLIENT_DATA_COMMAND(Add, (ReadBuffer&) key, number);
}

int Client::Append(const ReadBuffer& key, const ReadBuffer& value)
{
    CLIENT_DATA_COMMAND(Append, (ReadBuffer&) key, (ReadBuffer&) value);
}

int Client::Delete(const ReadBuffer& key)
{
    CLIENT_DATA_COMMAND(Delete, (ReadBuffer&) key);
}

int Client::Remove(const ReadBuffer& key)
{
    CLIENT_DATA_COMMAND(Remove, (ReadBuffer&) key);
}

int Client::ListKeys(const ReadBuffer& startKey, unsigned count, unsigned offset)
{
    CLIENT_DATA_COMMAND(ListKeys, (ReadBuffer&) startKey, count, offset);
}

int Client::ListKeyValues(const ReadBuffer& startKey, unsigned count, unsigned offset)
{
    CLIENT_DATA_COMMAND(ListKeyValues, (ReadBuffer&) startKey, count, offset);
}

int Client::Count(const ReadBuffer& startKey, unsigned count, unsigned offset)
{
    CLIENT_DATA_COMMAND(Count, (ReadBuffer&) startKey, count, offset);
}

int Client::Filter(const ReadBuffer& startKey, unsigned count, unsigned offset, uint64_t& commandID)
{
    Request*    req;                                
    
    CLIENT_MUTEX_GUARD_DECLARE();                   
    
    if (!isDatabaseSet || !isTableSet)              
        return SDBP_BADSCHEMA;                      

    if (isBatched)
        return SDBP_API_ERROR;
    
    commandID = NextCommandID();
    req = new Request;
    req->ListKeyValues(commandID, tableID, (ReadBuffer&) startKey, count, offset);
    req->async = true;
    requests.Append(req);
        
    result->Close();                                
    result->AppendRequest(req);                     
    
    CLIENT_MUTEX_GUARD_UNLOCK();                    
    EventLoop();                                    
    return result->GetCommandStatus();                 
}

int Client::Receive(uint64_t commandID)
{
    Request*    req;
    ReadBuffer  key;
    
    CLIENT_MUTEX_GUARD_DECLARE();                   
    
    if (!isDatabaseSet || !isTableSet)              
        return SDBP_BADSCHEMA;                      

    if (isBatched)
        return SDBP_API_ERROR;
    
    // create dummy request
    req = new Request;
    req->ListKeyValues(commandID, 0, key, 0, 0);
    req->async = true;

    result->Close();                                
    result->AppendRequest(req);                     
    
    CLIENT_MUTEX_GUARD_UNLOCK();                    
    EventLoop();                                    
    return result->GetCommandStatus();                 
}

int Client::Begin()
{
    Log_Trace();

    CLIENT_MUTEX_GUARD_DECLARE();

    result->Close();
    result->SetBatchLimit(batchLimit);
    isBatched = true;
    isReading = false;
    
    return SDBP_SUCCESS;
}

int Client::Submit()
{
    Log_Trace();

    EventLoop();
    isBatched = false;

    ClearQuorumRequests();
    requests.ClearMembers();
    
    return result->GetTransportStatus();
}

int Client::Cancel()
{
    Log_Trace();

    CLIENT_MUTEX_GUARD_DECLARE();

    requests.Clear();

    result->Close();
    isBatched = false;

    ClearQuorumRequests();
    requests.ClearMembers();
    
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
        result->SetTransportStatus(SDBP_API_ERROR);
        return;
    }
    
    GLOBAL_MUTEX_GUARD_DECLARE();
    
    EventLoop::UpdateTime();

    Log_Trace("%U", databaseID);
    Log_Trace("%U", tableID);
    
    // TODO: HACK this is here for enable async requests to receive the rest of response
    if (requests.GetLength() > 0)
    {
        AssignRequestsToQuorums();
        SendQuorumRequests();
    }
    
    EventLoop::UpdateTime();
    EventLoop::Reset(&globalTimeout);
    EventLoop::Reset(&masterTimeout);
    timeoutStatus = SDBP_SUCCESS;
    
    GLOBAL_MUTEX_GUARD_UNLOCK();
    
    while (!IsDone())
    {
        GLOBAL_MUTEX_GUARD_LOCK();        
        long sleep = EventLoop::RunTimers();
        if (IsDone())
        {
            GLOBAL_MUTEX_GUARD_UNLOCK();
            break;
        }
        

        if (!IOProcessor::Poll(sleep))
        {
            GLOBAL_MUTEX_GUARD_UNLOCK();
            break;
        }

        // let other threads enter IOProcessor and complete requests
        GLOBAL_MUTEX_GUARD_UNLOCK();
        YIELD();
    }

    CLIENT_MUTEX_LOCK();
    
    requests.Clear();
    
    result->SetConnectivityStatus(connectivityStatus);
    result->SetTimeoutStatus(timeoutStatus);
    result->Begin();
    
    CLIENT_MUTEX_UNLOCK();
}

bool Client::IsDone()
{
    CLIENT_MUTEX_GUARD_DECLARE();
    
    if (result->GetRequestCount() == 0 && configState != NULL)
        return true;
    
    if (result->GetTransportStatus() == SDBP_SUCCESS)
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
            Log_Debug("Node %d is the master", nodeID);
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
        Log_Debug("Node %d lost its mastership", nodeID);
        master = -1;
        connectivityStatus = SDBP_NOMASTER;

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

bool Client::IsReading()
{
    return isReading;
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
        ConfigureShardServers();
        AssignRequestsToQuorums();
        SendQuorumRequests();
    }
}

bool Client::AppendDataRequest(Request* req, int &status)
{
    req->isBulk = isBulkLoading;
    requests.Append(req);

    if (isBatched)
    {
        if (!result->AppendRequest(req) ||
         (requests.GetLength() > 1 && isReading && !req->IsReadRequest()) ||
         (requests.GetLength() > 1 && !isReading && req->IsReadRequest()))
        {
            requests.Clear();
            result->Close();
            isBatched = false;
            status = SDBP_API_ERROR;
            return true;
        }

        if (req->IsReadRequest())
            isReading = true;
        else
            isReading = false;

        status = SDBP_SUCCESS;
        return true;
    }

    if (req->IsReadRequest())
        isReading = true;
    else                                            
        isReading = false;

    result->Close();
    result->AppendRequest(req);

    return false;
}

void Client::ReassignRequest(Request* req)
{
    uint64_t        quorumID;
    ConfigQuorum*   quorum;
    ReadBuffer      key;
    Request*        subRequest;
    
    if (!configState)
        return;
    
    // handle controller requests
    if (req->IsControllerRequest())
    {
        if (master >= 0)
            controllerConnections[master]->Send(req);
        else
            requests.Append(req);

        return;
    }

    // multi requests won't be reassigned again, because they are not assigned to quorums
    // instead subRequests created as a clone of the multi request and subRequests are sent
    // to all quorums
    if (req->multi)
    {
        ClientResponse  resp;
        
        FOREACH (quorum, configState->quorums)
        {
            subRequest = new Request;
            // TODO: HACK copy the parent request ClientRequest part
            *((ClientRequest*) subRequest) = *((ClientRequest*) req);
            subRequest->next = subRequest->prev = subRequest;
            subRequest->commandID = NextCommandID();
            subRequest->quorumID = quorum->quorumID;
            subRequest->parent = req;
            result->AppendRequest(subRequest);

            AddRequestToQuorum(subRequest);
        }

        // simulate completion of the multi request with a fake response
        resp.commandID = req->commandID;
        if (req->type == CLIENTREQUEST_COUNT)
            resp.Number(0);
        else
            resp.OK();

        result->AppendRequestResponse(&resp);        
    }
    else
    {
        // find quorum by key
        key.Wrap(req->key);
        if (!GetQuorumID(req->tableID, key, quorumID))
            ASSERT_FAIL();

        // reassign the request to the new quorum
        req->quorumID = quorumID;

        quorum = configState->GetQuorum(quorumID);
        if (!IsReading() && quorum->hasPrimary == false)
            requests.Append(req);
        else
            AddRequestToQuorum(req);
    }
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

    FOREACH_FIRST (it, requestsCopy)
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
    
    ASSERT(configState != NULL);
    table = configState->GetTable(tableID);
    //Log_Trace("%U", tableID);
    ASSERT(table != NULL);
    FOREACH (it, table->shards)
    {
        shard = configState->GetShard(*it);
        if (shard == NULL)
            continue;

        firstKey.Wrap(shard->firstKey);
        lastKey.Wrap(shard->lastKey);

        if (GREATER_THAN(key, firstKey) && LESS_THAN(key, lastKey))
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

//    Log_Trace("qrequest length: %u, end = %s", qrequests->GetLength(), end ? "true" : "false");

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
    RequestList         bulkRequests;
    Request*            itRequest;
    uint64_t*           itNode;
    uint64_t            nodeID;
    unsigned            maxRequests;
    unsigned            numServed;
    bool                flushNeeded;

    // sanity checks
    if (conn->GetState() != TCPConnection::CONNECTED)
        return;

    if (!configState)
        return;

    // check quorums
    if (!quorumRequests.Get(quorumID, qrequests))
        return;
    
    quorum = configState->GetQuorum(quorumID);
    if (!quorum)
        ASSERT_FAIL();
    
    // with consistency level set to STRICT, send requests only to primary shard server
    if (!isBulkLoading && consistencyLevel == SDBP_CONSISTENCY_STRICT && 
     quorum->primaryID != conn->GetNodeID())
        return;
    
    // load balancing in relaxed consistency levels
    maxRequests = GetMaxQuorumRequests(qrequests, conn, quorum);

    numServed = 0;
    Log_Trace("quorumID: %U, numRequests: %u, maxRequests: %u, nodeID: %U", quorumID, qrequests->GetLength(), maxRequests, conn->GetNodeID());
    while (qrequests->GetLength() > 0)
    {   
        req = qrequests->First();
        qrequests->Remove(req);
        
        // handle bulk requests
        if (req->isBulk && quorum->activeNodes.GetLength() > 1)
        {
            // send to all shardservers before removing from quorum requests
            FOREACH (itNode, req->shardConns)
            {
                if (*itNode == conn->GetNodeID())
                    break;
            }

            if (itNode == NULL)
            {
                // send
                bulkRequests.Append(req);
                nodeID = conn->GetNodeID();
                req->shardConns.Append(nodeID);
            }
            else
                continue;   // don't send the request because it is already sent
        }
        
        // set paxosID by consistency level
        req->paxosID = GetRequestPaxosID();
 
        // send request
        numServed++;
        if (!conn->SendRequest(req))
        {
            flushNeeded = true;
            break;
        }
        
        // let other shardservers serve requests        
        if (maxRequests != 0 && numServed >= maxRequests)
        {
            flushNeeded = true;
            break;
        }
    }
    
    Log_Trace("quorumID: %U, numServed: %u, nodeID: %U", quorumID, numServed, conn->GetNodeID());

    if (qrequests->GetLength() == 0)
        flushNeeded = true;

    if (isBulkLoading && bulkRequests.GetLength() == 0)
        flushNeeded = true;
    
    if (flushNeeded)
        conn->Flush();
    
    // put back those requests to the quorum requests list that have not been sent to all
    // shardservers in bulk mode
    FOREACH_LAST (itRequest, bulkRequests)
    {
        bulkRequests.Remove(itRequest);
        qrequests->Prepend(itRequest);
    }
}

void Client::SendQuorumRequests()
{
    ShardConnection*        conn;
    uint64_t*               qit;
    RequestList*            qrequests;
    ShardConnection**       connArray;
    unsigned                i;
    int                     j;

    Log_Trace();

    // create an array for containing the connections in randomized order
    connArray = new ShardConnection*[shardConnections.GetCount()];

    i = 0;
    FOREACH (conn, shardConnections)
    {
        connArray[i] = conn;
        i++;
    }
    
    if (consistencyLevel != SDBP_CONSISTENCY_STRICT)
    {
        // Fisher–Yates shuffle, modern version
        for (i = shardConnections.GetCount() - 1; i >= 1; i--)
        {
            j = RandomInt(0, i);
            conn = connArray[j];
            connArray[j] = connArray[i];
            connArray[i] = conn;
        }
    }
    
    for (i = 0; i < shardConnections.GetCount(); i++)
    {
        conn = connArray[i];
        
        if (conn->IsWritePending())
            continue;

        Log_Trace("conn = %s, quorums.length = %u", conn->GetEndpoint().ToString(), conn->GetQuorumList().GetLength());
        FOREACH (qit, conn->GetQuorumList())
        {
            if (!quorumRequests.Get(*qit, qrequests))
                continue;
            SendQuorumRequest(conn, *qit);
        }
    }   

    delete[] connArray;
}

void Client::ClearQuorumRequests()
{
    RequestListMap::Node*   requestNode;
    RequestList*            requestList;
    Request*                request;

    FOREACH (requestNode, quorumRequests)
    {
        requestList = requestNode->Value();
        FOREACH_FIRST (request, *requestList)
            requestList->Remove(request);
    }
}

void Client::InvalidateQuorum(uint64_t quorumID, uint64_t nodeID)
{
    ConfigQuorum*       quorum;
    ShardConnection*    shardConn;
    uint64_t*           nit;
    
    quorum = configState->GetQuorum(quorumID);
    if (!quorum)
        ASSERT_FAIL();
        
    if (quorum->hasPrimary && quorum->primaryID == nodeID)
    {
        quorum->hasPrimary = false;
        quorum->primaryID = 0;
    
        // invalidate shard connections
        FOREACH (nit, quorum->activeNodes)
        {
            shardConn = shardConnections.Get<uint64_t>(*nit);
            ASSERT(shardConn != NULL);
            if (nodeID == shardConn->GetNodeID())
                shardConn->ClearQuorumMembership(quorumID);
        }
    }
    
    InvalidateQuorumRequests(quorumID);
}

void Client::InvalidateQuorumRequests(uint64_t quorumID)
{
    RequestList*        qrequests;

    qrequests = NULL;
    if (!quorumRequests.Get(quorumID, qrequests))
        return;
    
    // push back requests to unselected requests' queue
    requests.PrependList(*qrequests);
}

void Client::ConfigureShardServers()
{
    ConfigShardServer*          ssit;
    ConfigQuorum*               qit;
    ShardConnection*            shardConn;
    uint64_t*                   nit;
    Endpoint                    endpoint;
    
    FOREACH (ssit, configState->shardServers)
    {
        shardConn = shardConnections.Get(ssit->nodeID);
        if (shardConn == NULL)
        {
            // connect to previously unknown shard server
            endpoint = ssit->endpoint;
            endpoint.SetPort(ssit->sdbpPort);
            shardConn = new ShardConnection(this, ssit->nodeID, endpoint);
            shardConnections.Insert(shardConn);
        }
        else
        {
            // clear shard server quorum info
            Log_Trace("ssit: %s, shardConn: %s", ssit->endpoint.ToString(), shardConn->GetEndpoint().ToString());
            // TODO: remove this hack when shardserver's endpoint will be sent correctly in configState
            endpoint = ssit->endpoint;
            endpoint.SetPort(ssit->sdbpPort);
            ASSERT(endpoint == shardConn->GetEndpoint());
            shardConn->ClearQuorumMemberships();
        }
    }
    
    // assign quorums to ShardConnections
    FOREACH (qit, configState->quorums)
    {
        Log_Trace("quorumID: %U, primary: %U", qit->quorumID, qit->hasPrimary ? qit->primaryID : 0);
        FOREACH (nit, qit->activeNodes)
        {
            shardConn = shardConnections.Get(*nit);
            ASSERT(shardConn != NULL);
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

unsigned Client::GetMaxQuorumRequests(RequestList* qrequests, ShardConnection* conn, ConfigQuorum* quorum)
{
    unsigned            maxRequests;
    unsigned            totalRequests;
    ShardConnection*    otherConn;
    uint64_t*           itNode;
    
    // load balancing in relaxed consistency levels
    maxRequests = 0;
    totalRequests = 0;
    if (consistencyLevel == SDBP_CONSISTENCY_ANY || consistencyLevel == SDBP_CONSISTENCY_RYW)
    {
        totalRequests = qrequests->GetLength();
        
        // this correction can be ignored when there are no big batches
        FOREACH (itNode, quorum->activeNodes)
        {
            otherConn = shardConnections.Get(*itNode);
            if (otherConn == conn)
                continue;

            totalRequests += otherConn->GetNumSentRequests();
        }
        
        maxRequests = (unsigned) ceil(totalRequests / quorum->activeNodes.GetLength());
    }
    
    return maxRequests;
}    

uint64_t Client::GetRequestPaxosID()
{
    if (consistencyLevel == SDBP_CONSISTENCY_ANY)
        return 0;
    else if (consistencyLevel == SDBP_CONSISTENCY_RYW)
        return highestSeenPaxosID;
    else if (consistencyLevel == SDBP_CONSISTENCY_STRICT)
        return 1;
    else
        ASSERT_FAIL();
    
    return 0;
}

void Client::Lock()
{
    mutex.Lock();
}

void Client::Unlock()
{
    mutex.Unlock();
}

void Client::LockGlobal()
{
    globalMutex.Lock();
}

void Client::UnlockGlobal()
{
    globalMutex.Unlock();
}

bool Client::IsGlobalLocked()
{
    if (globalMutex.GetThreadID() == ThreadPool::GetThreadID())
        return true;
    return false;
}
