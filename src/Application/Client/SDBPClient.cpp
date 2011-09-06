#include "SDBPClient.h"
#include "SDBPControllerConnection.h"
#include "SDBPShardConnection.h"
#include "SDBPClientConsts.h"
#include "System/IO/IOProcessor.h"
#include "System/Threading/ThreadPool.h"
#include "Framework/Replication/PaxosLease/PaxosLease.h"
#include "Application/Common/ClientRequest.h"
#include "Application/Common/ClientResponse.h"

#define MAX_IO_CONNECTION               32768
#define DEFAULT_BATCH_LIMIT             (1*MB)

#define CLIENT_MULTITHREADED 
#ifdef CLIENT_MULTITHREADED

// globalMutex protects the underlying single threaded IO and Event handling layer
Mutex       globalMutex;                    
unsigned    numClients;
ThreadPool* ioThread;

#define CLIENT_MUTEX_GUARD_DECLARE()    MutexGuard clientMutexGuard(mutex)
#define CLIENT_MUTEX_GUARD_LOCK()       clientMutexGuard.Lock()
#define CLIENT_MUTEX_GUARD_UNLOCK()     clientMutexGuard.Unlock()

#define CLIENT_MUTEX_LOCK()             Lock()
#define CLIENT_MUTEX_UNLOCK()           Unlock()

#define GLOBAL_MUTEX_GUARD_DECLARE()    MutexGuard globalMutexGuard(globalMutex)
#define GLOBAL_MUTEX_GUARD_LOCK()       globalMutexGuard.Lock()
#define GLOBAL_MUTEX_GUARD_UNLOCK()     globalMutexGuard.Unlock()

#else // CLIENT_MULTITHREADED

#define CLIENT_MUTEX_GUARD_DECLARE()
#define CLIENT_MUTEX_GUARD_LOCK()
#define CLIENT_MUTEX_GUARD_UNLOCK()

#define CLIENT_MUTEX_LOCK()
#define CLIENT_MUTEX_UNLOCK()

#define GLOBAL_MUTEX_GUARD_DECLARE()
#define GLOBAL_MUTEX_GUARD_LOCK()
#define GLOBAL_MUTEX_GUARD_UNLOCK()

#endif // CLIENT_MULTITHREADED

#define VALIDATE_CONTROLLER()       \
    if (numControllers == 0)        \
        return SDBP_API_ERROR;


#define CLIENT_DATA_COMMAND(op, ...)                \
    Request*    req;                                \
                                                    \
    CLIENT_MUTEX_GUARD_DECLARE();                   \
                                                    \
    if (configState == NULL)						\
		return SDBP_API_ERROR;						\
													\
	req = new Request;                              \
    req->op(NextCommandID(), configState->paxosID,  \
     tableID, __VA_ARGS__);                         \
    AppendDataRequest(req);                         \
                                                    \
    CLIENT_MUTEX_GUARD_UNLOCK();                    \
    EventLoop();                                    \
    return result->GetCommandStatus();              \


#define CLIENT_DATA_PROXIED_COMMAND(op, ...)        \
    int         cmpres;                             \
    Request*    req;                                \
    Request*    it;                                 \
                                                    \
    CLIENT_MUTEX_GUARD_DECLARE();                   \
                                                    \
    if (configState == NULL)						\
		return SDBP_API_ERROR;						\
													\
    req = new Request;                              \
    req->op(NextCommandID(), configState->paxosID,  \
     tableID, __VA_ARGS__);                         \
                                                    \
    if (batchMode == SDBP_BATCH_NOAUTOSUBMIT &&     \
     proxySize + REQUEST_SIZE(req) >= batchLimit)   \
    {                                               \
        delete req;                                 \
        return SDBP_API_ERROR;                      \
    }                                               \
                                                    \
    it = proxiedRequests.Locate(req, cmpres);       \
    if (cmpres == 0 && it != NULL)                  \
    {                                               \
        proxySize -= REQUEST_SIZE(it);              \
        proxiedRequests.Delete(it);                 \
        ASSERT(proxySize >= 0);                     \
    }                                               \
    proxiedRequests.Insert<const Request*>(req);    \
    proxySize += REQUEST_SIZE(req);                 \
                                                    \
    CLIENT_MUTEX_GUARD_UNLOCK();                    \
                                                    \
    if (batchMode == SDBP_BATCH_SINGLE)             \
        return Submit();                            \
                                                    \
    if (batchMode == SDBP_BATCH_DEFAULT &&          \
     proxySize >= batchLimit)                       \
        return Submit();                            \
                                                    \
    return SDBP_SUCCESS;                            \


#define CLIENT_SCHEMA_COMMAND(op, ...)              \
    Request*    req;                                \
    int         status;                             \
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
    status = result->GetCommandStatus();            \
    return status;


using namespace SDBPClient;

static inline uint64_t Hash(uint64_t h)
{
    return h;
}

static inline uint64_t Key(const ShardConnection* conn)
{
    return conn->GetNodeID();
}

static inline const Request* Key(const Request* req)
{
    return req;
}

static inline int KeyCmp(uint64_t a, uint64_t b)
{
    if (a < b)
        return -1;
    if (a > b)
        return 1;
    return 0;
}

static int KeyCmp(const Request* a, const Request* b)
{
    if (a->tableID < b->tableID)
        return -1;
    if (a->tableID > b->tableID)
        return 1;
    
    return Buffer::Cmp(a->key, b->key);
}

Client::Client()
{
    controllerConnections = NULL;
    master = -1;
    commandID = 0;
    masterCommandID = 0;
    configState = NULL;
    databaseID = 0;
    numControllers = 0;
    globalTimeout.SetCallable(MFUNC(Client, OnGlobalTimeout));
    masterTimeout.SetCallable(MFUNC(Client, OnMasterTimeout));
    result = NULL;
    batchMode = SDBP_BATCH_DEFAULT;
    batchLimit = DEFAULT_BATCH_LIMIT;
    proxySize = 0;

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
    // sanity check on parameters
    if (nodec <= 0 || nodev == NULL)
        return SDBP_API_ERROR;

    GLOBAL_MUTEX_GUARD_DECLARE();
    // TODO: find out the optimal size of MAX_SERVER_NUM
    if (!IOProcessor::Init(MAX_IO_CONNECTION))
        return SDBP_API_ERROR;

    numClients++;
    if (numClients == 1)
    {
        ioThread = ThreadPool::Create(1);
        ioThread->Execute(MFUNC(Client, IOThreadFunc));
        ioThread->Start();
    }
    GLOBAL_MUTEX_GUARD_UNLOCK();

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
    consistencyMode = SDBP_CONSISTENCY_STRICT;
    highestSeenPaxosID = 0;
        
    return SDBP_SUCCESS;
}

void Client::Shutdown()
{
    if (!controllerConnections)
        return;
    
    Submit();

    isShutdown.SetWaiting(true);
    onClientShutdown.SetCallable(MFUNC(Client, OnClientShutdown));
    EventLoop::Add(&onClientShutdown);
    isShutdown.Wait();

    GLOBAL_MUTEX_GUARD_DECLARE();
    numClients--;
    if (numClients == 0)
    {
        EventLoop::Stop();
        ioThread->WaitStop();
        delete ioThread;
        ioThread = NULL;
    }
    GLOBAL_MUTEX_GUARD_UNLOCK();

    IOProcessor::Shutdown();    
}

void Client::OnClientShutdown()
{
    RequestListMap::Node*   requestNode;
    RequestList*            requestList;
    ShardConnection*        shardConnection;

    for (int i = 0; i < numControllers; i++)
        controllerConnections[i]->Close();

    FOREACH (shardConnection, shardConnections)
        shardConnection->Close();
    
    EventLoop::Remove(&masterTimeout);
    EventLoop::Remove(&globalTimeout);


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

    isShutdown.Wake();
}

void Client::SetBatchLimit(unsigned batchLimit_)
{
    batchLimit = batchLimit_;
}

void Client::SetConsistencyMode(int consistencyMode_)
{
    consistencyMode = consistencyMode_;
}

void Client::SetBatchMode(int batchMode_)
{
    batchMode = batchMode_;
}

void Client::SetGlobalTimeout(uint64_t timeout)
{
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

ConfigState* Client::GetConfigState()
{
    Log_Trace();

    CLIENT_MUTEX_GUARD_DECLARE();

    if (numControllers == 0)
        return NULL;
    
    if (!configState)
    {
        result->Close();
        CLIENT_MUTEX_UNLOCK();
        EventLoop();
        CLIENT_MUTEX_LOCK();
    }

    return configState;
}

void Client::WaitConfigState()
{
    CLIENT_MUTEX_GUARD_DECLARE();

    if (numControllers == 0)
        return;

    // delete configState, so EventLoop() will wait for a new one
    configState = NULL;
    result->Close();
    CLIENT_MUTEX_UNLOCK();
    EventLoop();
    CLIENT_MUTEX_LOCK();
}

Result* Client::GetResult()
{
	CLIENT_MUTEX_GUARD_DECLARE();

    Result* tmp;
    
    tmp = result;
    result = new Result;
    return tmp;
}

int Client::GetTransportStatus()
{
    return result->GetTransportStatus();
}

int Client::GetConnectivityStatus()
{
    return connectivityStatus;
}

int Client::GetTimeoutStatus()
{
    return timeoutStatus;
}

int Client::GetCommandStatus()
{
    return result->GetCommandStatus();
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

#define GET_CONFIG_STATE_OR_RETURN(...) \
    CLIENT_MUTEX_GUARD_DECLARE();       \
                                        \
    if (numControllers == 0)            \
        return __VA_ARGS__;             \
                                        \
    if (!configState)                   \
    {                                   \
        result->Close();                \
        CLIENT_MUTEX_UNLOCK();          \
        EventLoop();                    \
        CLIENT_MUTEX_LOCK();            \
    }                                   \
    if (!configState)                   \
        return __VA_ARGS__;

unsigned Client::GetNumQuorums()
{
    GET_CONFIG_STATE_OR_RETURN(0);
    
    return configState->quorums.GetLength();
}

uint64_t Client::GetQuorumIDAt(unsigned n)
{
    ConfigQuorum*   quorum;
    unsigned        i;

    GET_CONFIG_STATE_OR_RETURN(0);
    
    i = 0;
    FOREACH (quorum, configState->quorums)
    {
        if (i == n)
            return quorum->quorumID;
            
        i++;
    }

    return 0;
}

void Client::GetQuorumNameAt(unsigned n, Buffer& name)
{
    ConfigQuorum*   quorum;
    unsigned        i;

    GET_CONFIG_STATE_OR_RETURN();
    
    i = 0;
    FOREACH (quorum, configState->quorums)
    {
        if (i == n)
        {
            name.Write(quorum->name);
            return;
        }
            
        i++;
    }
}

unsigned Client::GetNumDatabases()
{
    GET_CONFIG_STATE_OR_RETURN(0);
    
    return configState->databases.GetLength();
}

uint64_t Client::GetDatabaseIDAt(unsigned n)
{
    ConfigDatabase* database;
    unsigned        i;

    GET_CONFIG_STATE_OR_RETURN(0);
    
    i = 0;
    FOREACH (database, configState->databases)
    {
        if (i == n)
            return database->databaseID;
            
        i++;
    }

    return 0;
}

void Client::GetDatabaseNameAt(unsigned n, Buffer& name)
{
    ConfigDatabase* database;
    unsigned        i;

    GET_CONFIG_STATE_OR_RETURN();

    i = 0;
    FOREACH (database, configState->databases)
    {
        if (i == n)
        {
            name.Write(database->name);
            return;
        }
            
        i++;
    }
}

unsigned Client::GetNumTables(uint64_t databaseID)
{
    ConfigDatabase*     database;
    
    GET_CONFIG_STATE_OR_RETURN(0);

    database = NULL;
    FOREACH (database, configState->databases)
    {
        if (database->databaseID == databaseID)
            break;
    }

    if (!database)
        return 0;
    
    return database->tables.GetLength();
}

uint64_t Client::GetTableIDAt(uint64_t databaseID, unsigned n)
{
    ConfigDatabase* database;
    uint64_t*       itTableID;
    unsigned        i;

    GET_CONFIG_STATE_OR_RETURN(0);
    
    database = NULL;
    FOREACH (database, configState->databases)
    {
        if (database->databaseID == databaseID)
            break;
    }

    if (!database)
        return 0;

    i = 0;
    FOREACH (itTableID, database->tables)
    {
        if (i == n)
            return *itTableID;
        i++;
    }
    
    return 0;
}

void Client::GetTableNameAt(uint64_t databaseID, unsigned n, Buffer& name)
{
    ConfigDatabase* database;
    ConfigTable*    table;
    uint64_t*       itTableID;
    unsigned        i;

    GET_CONFIG_STATE_OR_RETURN();
    
    database = NULL;
    FOREACH (database, configState->databases)
    {
        if (database->databaseID == databaseID)
            break;
    }

    if (!database)
        return;
    
    i = 0;
    FOREACH (itTableID, database->tables)
    {
        if (i == n)
            break;
        i++;
    }
    
    if (!itTableID)
        return;
    
    FOREACH (table, configState->tables)
    {
        if (table->tableID == *itTableID)
        {
            name.Write(table->name);
            return;
        }
    }
}    

int Client::Get(uint64_t tableID, const ReadBuffer& key)
{
    int         cmpres;
    Request*    req;
    Request*    it;
    
    CLIENT_MUTEX_GUARD_DECLARE();
    
    req = new Request;
    req->Get(NextCommandID(), configState->paxosID, tableID, (ReadBuffer&) key);

    // find
    it = proxiedRequests.Locate(req, cmpres);
    if (cmpres == 0 && it != NULL)
    {
        delete req;
        if (it->type == CLIENTREQUEST_SET)
        {
            result->proxied = true;
            result->proxiedValue.Wrap(it->value);
            return SDBP_SUCCESS;
        }
        else if (it->type == CLIENTREQUEST_DELETE)
        {
            return SDBP_FAILED;
        }
        else
            ASSERT_FAIL();
    }
        
    AppendDataRequest(req);

    CLIENT_MUTEX_GUARD_UNLOCK();
    EventLoop();
    return result->GetCommandStatus();
}

int Client::Set(uint64_t tableID, const ReadBuffer& key, const ReadBuffer& value)
{
//    CLIENT_DATA_PROXIED_COMMAND(Set, (ReadBuffer&) key, (ReadBuffer&) value);
    int         cmpres;                             
    Request*    req;                                
    Request*    it;                                 
                                                    
    CLIENT_MUTEX_GUARD_DECLARE();                   
    
	if (configState == NULL)
		return SDBP_API_ERROR;

    req = new Request;                              
    req->Set(NextCommandID(), configState->paxosID,  
     tableID, (ReadBuffer&) key, (ReadBuffer&) value);                         
                                                    
    if (batchMode == SDBP_BATCH_NOAUTOSUBMIT &&     
     proxySize + REQUEST_SIZE(req) >= batchLimit)   
    {                                               
        delete req;                                 
        return SDBP_API_ERROR;                      
    }                                               
                                                    
    it = proxiedRequests.Locate(req, cmpres);       
    if (cmpres == 0 && it != NULL)                  
    {                                               
        proxySize -= REQUEST_SIZE(it);              
        proxiedRequests.Delete(it);
        ASSERT(proxySize >= 0);                     
    }                                               
    proxiedRequests.Insert<const Request*>(req);    
    proxySize += REQUEST_SIZE(req);                 
                                                    
    CLIENT_MUTEX_GUARD_UNLOCK();                    
                                                    
    if (batchMode == SDBP_BATCH_SINGLE)             
        return Submit();                            
                                                    
    if (batchMode == SDBP_BATCH_DEFAULT &&          
     proxySize >= batchLimit)                       
        return Submit();
                                                    
    return SDBP_SUCCESS;                            
}

int Client::Add(uint64_t tableID, const ReadBuffer& key, int64_t number)
{
    Request*    req;
    Request*    itRequest;
    ReadBuffer  requestKey;

    CLIENT_MUTEX_GUARD_DECLARE();

    result->Close();
    FOREACH(itRequest, proxiedRequests)
    {
        if (itRequest->tableID != tableID)
            continue;
        
        requestKey.Wrap(itRequest->key);
        if (ReadBuffer::Cmp(key, requestKey) != 0)
            continue;
            
        proxiedRequests.Remove(itRequest);
        requests.Append(itRequest);
        result->AppendRequest(itRequest);
        proxySize -= REQUEST_SIZE(itRequest);
        ASSERT(proxySize >= 0);
        break;
    }

    req = new Request;
    req->Add(NextCommandID(), configState->paxosID, tableID, (ReadBuffer&) key, number);
    requests.Append(req);
    result->AppendRequest(req);
    CLIENT_MUTEX_GUARD_UNLOCK();
    EventLoop();
	CLIENT_MUTEX_GUARD_LOCK();
    if (result->GetCommandStatus() == SDBP_SUCCESS)
    {
        for (itRequest = result->requests.First(); itRequest != NULL; /* advanced in body */)
        {
            if (itRequest->commandID != req->commandID)
                itRequest = result->requests.Remove(itRequest);
            else
                itRequest = result->requests.Next(itRequest);
        }
    }
    result->Begin();
    return result->GetCommandStatus();
}

int Client::Delete(uint64_t tableID, const ReadBuffer& key)
{
    CLIENT_DATA_PROXIED_COMMAND(Delete, (ReadBuffer&) key);
}

int Client::ListKeys(
 uint64_t tableID,
 const ReadBuffer& startKey, const ReadBuffer& endKey, const ReadBuffer& prefix,
 unsigned count, bool skip)
{
    Request*    req;

    CLIENT_MUTEX_GUARD_DECLARE();

	if (configState == NULL)
		return SDBP_API_ERROR;

    req = new Request;
    req->userCount = count;
    req->skip = skip;
    req->ListKeys(NextCommandID(), configState->paxosID, tableID,
     (ReadBuffer&) startKey, (ReadBuffer&) endKey, (ReadBuffer&) prefix, count);

    if (req->userCount > 0)
    {
        // fetch more from server in case proxied deletes override
        req->count += NumProxiedDeletes(req);
        if (skip)
            req->count++; // fetch one more from server in case of skip
    }

    AppendDataRequest(req);

    CLIENT_MUTEX_GUARD_UNLOCK();
    EventLoop();
	CLIENT_MUTEX_GUARD_LOCK();

    req->count = req->userCount;
    ComputeListResponse();

    return result->GetCommandStatus();
}

int Client::ListKeyValues(
 uint64_t tableID,
 const ReadBuffer& startKey, const ReadBuffer& endKey, const ReadBuffer& prefix,
 unsigned count, bool skip)
{
    Request*    req;

    CLIENT_MUTEX_GUARD_DECLARE();

    if (configState == NULL)
		return SDBP_API_ERROR;

    req = new Request;
    req->userCount = count;
    req->skip = skip;
    req->ListKeyValues(NextCommandID(), configState->paxosID, tableID,
     (ReadBuffer&) startKey, (ReadBuffer&) endKey, (ReadBuffer&) prefix, count);

    if (req->userCount > 0)
    {
        // fetch more from server in case proxied deletes override
        req->count += NumProxiedDeletes(req);
        if (skip)
            req->count++; // fetch one more from server in case of skip
    }

    AppendDataRequest(req);

    CLIENT_MUTEX_GUARD_UNLOCK();
    EventLoop();
	CLIENT_MUTEX_GUARD_LOCK();

    req->count = req->userCount;
    ComputeListResponse();

    return result->GetCommandStatus();
}

int Client::Count(
 uint64_t tableID,
 const ReadBuffer& startKey, const ReadBuffer& endKey, const ReadBuffer& prefix)
{
    CLIENT_DATA_COMMAND(Count,
     (ReadBuffer&) startKey, (ReadBuffer&) endKey, (ReadBuffer&) prefix);
}

int Client::Begin()
{
    Log_Trace();

    CLIENT_MUTEX_GUARD_DECLARE();

    result->Close();
   
    return SDBP_SUCCESS;
}

int Client::Submit()
{
    Request*    it;
    int         transportStatus;

    CLIENT_MUTEX_GUARD_DECLARE();

    if (proxiedRequests.GetCount() == 0)
        return SDBP_SUCCESS;
    
    Log_Trace();

    CLIENT_MUTEX_UNLOCK();
    Begin();
    CLIENT_MUTEX_LOCK();
    
    FOREACH_POP(it, proxiedRequests)
    {
        requests.Append(it);
        result->AppendRequest(it);
        proxySize -= REQUEST_SIZE(it);
    }
	ASSERT(proxySize == 0);

    CLIENT_MUTEX_UNLOCK();
    EventLoop();
    CLIENT_MUTEX_LOCK();

    ClearQuorumRequests();
    requests.ClearMembers();
    
    Log_Trace("Submit returning");
    
    transportStatus = result->GetTransportStatus();
    return transportStatus;
}

int Client::Cancel()
{
    Log_Trace();

    CLIENT_MUTEX_GUARD_DECLARE();

    ClearRequests();
    result->Close();

    return SDBP_SUCCESS;
}

void Client::ClearRequests()
{
    ShardConnection*    shardConnection;

    proxiedRequests.Clear();
    proxySize = 0;

    requests.Clear();
    ClearQuorumRequests();

    FOREACH (shardConnection, shardConnections)
        shardConnection->ClearRequests();

    for (int i = 0; i < numControllers; i++)
        controllerConnections[i]->ClearRequests();
}

/*
 * If wait is negative, EventLoop waits until all request is served or timeout happens.
 * If wait is zero, EventLoop runs exactly once and handles timer and IO events.
 * If wait is positive, EventLoop waits until all request is served ot timeout happens, but waits at most wait msecs.
 */
void Client::EventLoop()
{
    uint64_t		startTime;
    
    CLIENT_MUTEX_LOCK();

	if (!controllerConnections)
    {
        result->SetTransportStatus(SDBP_API_ERROR);
        return;
    }

    // avoid race conditions
    isDone.SetWaiting(true);
    if (requests.GetLength() > 0)
    {
        AssignRequestsToQuorums();
        SendQuorumRequests();
    }
    timeoutStatus = SDBP_SUCCESS;
    CLIENT_MUTEX_UNLOCK();
    
    EventLoop::Reset(&globalTimeout);
    if (master == -1)
        EventLoop::Reset(&masterTimeout);
    startTime = EventLoop::Now();
    
    isDone.Wait(); // wait for IO thread to process ops
    
    CLIENT_MUTEX_LOCK();    
    ClearRequests();
    result->SetConnectivityStatus(connectivityStatus);
    result->SetTimeoutStatus(timeoutStatus);
    result->Begin();
    CLIENT_MUTEX_UNLOCK();
    
    Log_Trace("EventLoop() returning");
}

// This should only be called with client mutex locked!
bool Client::IsDone()
{
    if (result->GetRequestCount() == 0 && configState != NULL)
    {
        return true;
    }
    
    if (result->GetTransportStatus() == SDBP_SUCCESS)
    {
        return true;
    }
    
    if (timeoutStatus != SDBP_SUCCESS)
    {
        return true;
    }
    
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

int64_t Client::GetMaster()
{
    return master;
}

void Client::SetMaster(int64_t master_, uint64_t nodeID)
{
    Log_Trace("known master: %I, set master: %I, nodeID: %U", master, master_, nodeID);
    
    if (master_ == (int64_t) nodeID)
    {
        if (master != master_)
        {
            // node became the master
            Log_Debug("Node %d is the master", nodeID);
            master = master_;
            connectivityStatus = SDBP_SUCCESS;
            EventLoop::Remove(&masterTimeout);
        }
    }
    else if (master_ < 0 && master == (int64_t) nodeID)
    {
        // node lost its mastership
        Log_Debug("Node %d lost its mastership", nodeID);
        master = -1;
        connectivityStatus = SDBP_NOMASTER;

        EventLoop::Reset(&masterTimeout);
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

    CLIENT_MUTEX_GUARD_DECLARE();

    timeoutStatus = SDBP_GLOBAL_TIMEOUT;
    
    TryWake();
}

void Client::OnMasterTimeout()
{
    Log_Trace();

    CLIENT_MUTEX_GUARD_DECLARE();
    
    timeoutStatus = SDBP_MASTER_TIMEOUT;
    
    TryWake();
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

void Client::AppendDataRequest(Request* req)
{
    requests.Append(req);
    result->Close();
    result->AppendRequest(req);
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
        if (!req->IsReadRequest() && quorum && quorum->hasPrimary == false)
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
    if (consistencyMode == SDBP_CONSISTENCY_STRICT && quorum->primaryID != conn->GetNodeID())
        return;

    // load balancing in relaxed consistency levels
    maxRequests = GetMaxQuorumRequests(qrequests, conn, quorum);

    numServed = 0;
    Log_Trace("quorumID: %U, numRequests: %u, maxRequests: %u, nodeID: %U", quorumID, qrequests->GetLength(), maxRequests, conn->GetNodeID());
    while (qrequests->GetLength() > 0)
    {   
        req = qrequests->First();
		if (req->IsShardServerRequest() && !req->IsReadRequest() && quorum->primaryID != conn->GetNodeID())
			break;
        qrequests->Remove(req);
        
        req->shardConns.Clear();
        nodeID = conn->GetNodeID();
        req->shardConns.Append(nodeID);
        
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

    if (flushNeeded)
        conn->Flush();    
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
    
    if (shardConnections.GetCount() == 0)
        return;

    // create an array for containing the connections in randomized order
    connArray = new ShardConnection*[shardConnections.GetCount()];

    i = 0;
    FOREACH (conn, shardConnections)
    {
        connArray[i] = conn;
        i++;
    }
    
    if (consistencyMode != SDBP_CONSISTENCY_STRICT)
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

        Log_Trace("conn nodeID %U, quorums.length = %u", conn->GetNodeID(), conn->GetQuorumList().GetLength());
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

void Client::NextRequest(
 Request* req, ReadBuffer nextShardKey, ReadBuffer endKey, ReadBuffer prefix,
 uint64_t count)
{
    ConfigTable*    configTable;
    ConfigShard*    configShard;
    uint64_t*       itShard;
    uint64_t        nextShardID;
    ReadBuffer      minKey;
    
    Log_Trace("count: %U, nextShardKey: %R", count, &nextShardKey);
    
    if (req->count > 0)
        req->count = count;

    configTable = configState->GetTable(req->tableID);
    ASSERT(configTable != NULL);
    
    nextShardID = 0;
    FOREACH (itShard, configTable->shards)
    {
        configShard = configState->GetShard(*itShard);

        // find the next shard that has the given nextShardKey as first key
        if (ReadBuffer::Cmp(configShard->firstKey, nextShardKey) == 0)
        {
            minKey = configShard->firstKey;
            nextShardID = configShard->shardID;
            break;
        }
    }

    Log_Trace("nextShardID: %U, minKey: %R", nextShardID, &minKey);
    req->endKey.Write(endKey);
    req->prefix.Write(prefix);
    if (nextShardID != 0)
        req->key.Write(minKey);
}

void Client::ConfigureShardServers()
{
    ConfigShardServer*          ssit;
    ConfigQuorum*               qit;
    ShardConnection*            shardConn;
    uint64_t*                   nit;
    Endpoint                    endpoint;
    
    Log_Trace("1");
    FOREACH (ssit, configState->shardServers)
    {
        shardConn = shardConnections.Get(ssit->nodeID);
        Log_Trace("%U", ssit->nodeID);
        if (shardConn == NULL)
        {
            Log_Trace("connect");
            // connect to previously unknown shard server
            if (ssit->sdbpPort != 0)
            {
                endpoint = ssit->endpoint;
                endpoint.SetPort(ssit->sdbpPort);
                shardConn = new ShardConnection(this, ssit->nodeID, endpoint);
                shardConnections.Insert<uint64_t>(shardConn);
            }
        }
        else
        {
            // clear shard server quorum info
            Log_Trace("ssit: %s, shardConn: %s", ssit->endpoint.ToString(), shardConn->GetEndpoint().ToString());
            if (ssit->sdbpPort != 0)
            {
                endpoint = ssit->endpoint;
                endpoint.SetPort(ssit->sdbpPort);
                endpoint.ToString();
                ASSERT(endpoint == shardConn->GetEndpoint());
            }
            shardConn->ClearQuorumMemberships();
        }
    }
    Log_Trace("2");
    
    // assign quorums to ShardConnections
    FOREACH (qit, configState->quorums)
    {
        Log_Trace("quorumID: %U, primary: %U", qit->quorumID, qit->hasPrimary ? qit->primaryID : 0);
        FOREACH (nit, qit->activeNodes)
        {
            Log_Trace("%U", *nit);
            shardConn = shardConnections.Get(*nit);
            //ASSERT(shardConn != NULL);
            // the controller always reports the last shard server in a quorum as active,
            // but that doesn't mean it is up and running
            if (shardConn != NULL)
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

unsigned Client::GetMaxQuorumRequests(
 RequestList* qrequests, ShardConnection* conn, ConfigQuorum* quorum)
{
    unsigned            maxRequests;
    unsigned            totalRequests;
    ShardConnection*    otherConn;
    uint64_t*           itNode;
    
    // load balancing in relaxed consistency levels
    maxRequests = 0;
    totalRequests = 0;
    if (consistencyMode == SDBP_CONSISTENCY_ANY || consistencyMode == SDBP_CONSISTENCY_RYW)
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
        
        maxRequests = (unsigned) ceil((double)totalRequests / quorum->activeNodes.GetLength());
    }
    
    return maxRequests;
}    

uint64_t Client::GetRequestPaxosID()
{
    if (consistencyMode == SDBP_CONSISTENCY_ANY)
        return 0;
    else if (consistencyMode == SDBP_CONSISTENCY_RYW)
        return highestSeenPaxosID;
    else if (consistencyMode == SDBP_CONSISTENCY_STRICT)
        return 1;
    else
        ASSERT_FAIL();
    
    return 0;
}

void Client::ComputeListResponse()
{
    bool                    isDelete;
    unsigned                i;
    unsigned                len;
    int                     cmp;
    List<ReadBuffer>        proxyKeys;
    List<ReadBuffer>        proxyValues;
    List<bool>              proxyDeletes;
    List<ReadBuffer>        serverKeys;
    List<ReadBuffer>        serverValues;
    List<ReadBuffer>        mergedKeys;
    List<ReadBuffer>        mergedValues;
    Request*                request;
    Request*                itProxyRequest;
    ClientResponse*         response;
    ReadBuffer              key;
    ReadBuffer              value;
    ReadBuffer*             itProxyKey;
    ReadBuffer*             itProxyValue;
    bool*                   itProxyDelete;
    ReadBuffer*             itServerKey;
    ReadBuffer*             itServerValue;
    ReadBuffer*             itKey;
    ReadBuffer*             itValue;
    ClientResponse**        itResponse;
 
    if (result->GetRequestCount() == 0)
        return;

    if (proxiedRequests.GetCount() == 0)
        return;
    
    request = result->GetRequestCursor();

    // compute proxied result into proxyKeys/proxyValues
    FOREACH(itProxyRequest, proxiedRequests)
    {
        if (itProxyRequest->tableID < request->tableID)
            continue;
        if (itProxyRequest->tableID > request->tableID)
            break;
        if (request->endKey.GetLength() > 0)
        {
            cmp = Buffer::Cmp(itProxyRequest->key, request->endKey);
            if (cmp >= 0)
                break;
        }
        
        if (itProxyRequest->key.BeginsWith(request->prefix))
        {
            cmp = Buffer::Cmp(itProxyRequest->key, request->key /*startKey*/);
            if (cmp == 0 && request->skip)
                continue;
            else if (cmp >= 0)
            {
                key.Wrap(itProxyRequest->key);
                proxyKeys.Append(key);
                isDelete = (itProxyRequest->type == CLIENTREQUEST_DELETE);
                proxyDeletes.Append(isDelete);
                if (request->type == CLIENTREQUEST_LIST_KEYVALUES && !isDelete)
                {
                    value.Wrap(itProxyRequest->value);
                    proxyValues.Append(value);
                }
            }
        }
    }

    // fetch result returned by servers into serverKeys/serverValues
    for (result->Begin(); !result->IsEnd(); result->Next())
    {
        result->GetKey(key);

        cmp = ReadBuffer::Cmp(key, request->key /*startKey*/);
        if (cmp == 0 && request->skip)
            continue;

        serverKeys.Append(key);
        if (request->type == CLIENTREQUEST_LIST_KEYVALUES)
        {
            result->GetValue(value);
            serverValues.Append(value);
        }
    }
    result->Begin(); // to reset Result requestCursor
    
    // merge
#define ADVANCE_PROXY()                                         \
    {                                                           \
        itProxyKey = proxyKeys.Next(itProxyKey);                \
        if (request->type == CLIENTREQUEST_LIST_KEYVALUES &&    \
         *itProxyDelete == false)                               \
            itProxyValue = proxyValues.Next(itProxyValue);      \
        itProxyDelete = proxyDeletes.Next(itProxyDelete);       \
    }
#define ADVANCE_SERVER()                                        \
    {                                                           \
        itServerKey = serverKeys.Next(itServerKey);             \
        if (request->type == CLIENTREQUEST_LIST_KEYVALUES)      \
            itServerValue = serverValues.Next(itServerValue);   \
    }
#define APPEND_PROXY()                                          \
    {                                                           \
        mergedKeys.Append(*itProxyKey);                         \
        len += itProxyKey->GetLength();                         \
        if (request->type == CLIENTREQUEST_LIST_KEYVALUES)      \
        {                                                       \
            mergedValues.Append(*itProxyValue);                 \
            len += itProxyValue->GetLength();                   \
        }                                                       \
    }
#define APPEND_SERVER()                                         \
    {                                                           \
        mergedKeys.Append(*itServerKey);                        \
        len += itServerKey->GetLength();                        \
        if (request->type == CLIENTREQUEST_LIST_KEYVALUES)      \
        {                                                       \
            mergedValues.Append(*itServerValue);                \
            len += itServerValue->GetLength();                  \
        }                                                       \
    }

    itProxyKey = proxyKeys.First();
    itProxyValue = proxyValues.First();
    itProxyDelete = proxyDeletes.First();
    itServerKey = serverKeys.First();
    itServerValue = serverValues.First();
    len = 0;
    while(true)
    {
        if (request->count > 0 && mergedKeys.GetLength() == request->count)
            break;
        if (itProxyKey == NULL && itServerKey == NULL)
            break;
        if (itProxyKey == NULL)
        {
            APPEND_SERVER();
            ADVANCE_SERVER();
            continue;
        }
        else if (itServerKey == NULL)
        {
            if (*itProxyDelete == false)
                APPEND_PROXY();
            ADVANCE_PROXY();
            continue;
        }
        Log_Trace("%.*s", itProxyKey->GetLength(), itProxyKey->GetBuffer());
        cmp = ReadBuffer::Cmp(*itProxyKey, *itServerKey);
        if (cmp == 0)
        {
            if (*itProxyDelete == false)
                APPEND_PROXY();
            ADVANCE_PROXY();
            ADVANCE_SERVER();
        }
        else if (cmp < 0)
        {
            APPEND_PROXY();
            ADVANCE_PROXY();
        }
        else
        {
            APPEND_SERVER();
            ADVANCE_SERVER();
        }
    }
    
    // construct new response
    request->responses.Clear();
    response = new ClientResponse;
    response->commandID = request->commandID;
    response->valueBuffer = new Buffer();
    response->valueBuffer->Allocate(len);
    if (request->type == CLIENTREQUEST_LIST_KEYS)
        response->type = CLIENTRESPONSE_LIST_KEYS;
    else
        response->type = CLIENTRESPONSE_LIST_KEYVALUES;

    response->keys = new ReadBuffer[mergedKeys.GetLength()];
    response->values = new ReadBuffer[mergedValues.GetLength()];
    response->numKeys = mergedKeys.GetLength();
    for (i = 0, itKey = mergedKeys.First(), itValue = mergedValues.First();
     itKey != NULL;
     itKey = mergedKeys.Next(itKey), i++)
    {
        key.Wrap(response->valueBuffer->GetPosition(), itKey->GetLength());
        response->valueBuffer->Append(*itKey);
        response->keys[i] = key;
        if (request->type == CLIENTREQUEST_LIST_KEYVALUES)
        {
            value.Wrap(response->valueBuffer->GetPosition(), itValue->GetLength());
            response->valueBuffer->Append(*itValue);
            response->values[i] = value;
            itValue = mergedValues.Next(itValue);
        }
    }    

    // delete server responses
    FOREACH(itResponse, request->responses)
        delete *itResponse;

    request->responses.Append(response);
}

uint64_t Client::NumProxiedDeletes(Request* request)
{
    int         cmp;
    uint64_t    count;
    Request*    itProxyRequest;
    
    count = 0;
    FOREACH(itProxyRequest, proxiedRequests)
    {
        if (itProxyRequest->tableID < request->tableID)
            continue;
        if (itProxyRequest->tableID > request->tableID)
            break;
        if (request->endKey.GetLength() > 0)
        {
            cmp = Buffer::Cmp(itProxyRequest->key, request->endKey);
            if (cmp >= 0)
                break;
        }
        
        if (itProxyRequest->key.BeginsWith(request->prefix))
        {
            cmp = Buffer::Cmp(itProxyRequest->key, request->key /*startKey*/);
            if (cmp >= 0)
                count++;
        }
    }
    
    return count;
}

void Client::TryWake()
{
    if (!isDone.IsWaiting())
        return;
    
    if (IsDone())
        isDone.Wake();
}

void Client::IOThreadFunc()
{
    long    sleep;
    
    EventLoop::Start();

    while(EventLoop::IsRunning())
    {
        sleep = EventLoop::RunTimers();
    
        if (sleep < 0 || sleep > SLEEP_MSEC)
            sleep = SLEEP_MSEC;
    
        if (!IOProcessor::Poll(sleep))
            break;
    }

    Log_Debug("IOThreadFunc finished");
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
