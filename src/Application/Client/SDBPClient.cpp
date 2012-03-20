#include "SDBPClient.h"
#include "SDBPControllerConnection.h"
#include "SDBPController.h"
#include "SDBPShardConnection.h"
#include "SDBPPooledShardConnection.h"
#include "SDBPClientConsts.h"
#include "System/Common.h"
#include "System/Macros.h"
#include "System/IO/IOProcessor.h"
#include "System/Threading/ThreadPool.h"
#include "System/Threading/LockGuard.h"
#include "Framework/Replication/PaxosLease/PaxosLease.h"
#include "Application/Common/ClientRequest.h"
#include "Application/Common/ClientResponse.h"

// TODO: find out the optimal size
#define MAX_IO_CONNECTION               32768
#define DEFAULT_BATCH_LIMIT             (1*MB)

#ifndef CLIENT_MULTITHREADED
#define CLIENT_MULTITHREADED
#endif

#ifdef CLIENT_MULTITHREADED

// globalMutex protects the underlying single threaded IO and Event handling layer
Mutex                                   globalMutex;                    
unsigned                                numClients = 0;
ThreadPool*                             ioThread = NULL;
Signal                                  ioThreadSignal;

#define CLIENT_MUTEX_GUARD_DECLARE()    MutexGuard clientMutexGuard(mutex)
#define CLIENT_MUTEX_GUARD_LOCK()       clientMutexGuard.Lock()
#define CLIENT_MUTEX_GUARD_UNLOCK()     clientMutexGuard.Unlock()

#define CLIENT_MUTEX_LOCK()             Lock()
#define CLIENT_MUTEX_UNLOCK()           Unlock()

#define GLOBAL_MUTEX_GUARD_DECLARE()    MutexGuard globalMutexGuard(globalMutex)
#define GLOBAL_MUTEX_GUARD_LOCK()       globalMutexGuard.Lock()
#define GLOBAL_MUTEX_GUARD_UNLOCK()     globalMutexGuard.Unlock()

#define GLOBAL_MUTEX_LOCK()             globalMutex.Lock()
#define GLOBAL_MUTEX_UNLOCK()           globalMutex.Unlock()

#else // CLIENT_MULTITHREADED

#define CLIENT_MUTEX_GUARD_DECLARE()
#define CLIENT_MUTEX_GUARD_LOCK()
#define CLIENT_MUTEX_GUARD_UNLOCK()

#define CLIENT_MUTEX_LOCK()
#define CLIENT_MUTEX_UNLOCK()

#define GLOBAL_MUTEX_GUARD_DECLARE()
#define GLOBAL_MUTEX_GUARD_LOCK()
#define GLOBAL_MUTEX_GUARD_UNLOCK()

#define GLOBAL_MUTEX_LOCK()
#define GLOBAL_MUTEX_UNLOCK()

#endif // CLIENT_MULTITHREADED

#define VALIDATE_CONTROLLERS()       \
    if (controller == 0)             \
        return SDBP_API_ERROR;

#define VALIDATE_CONFIG_STATE()     \
    if (configState == NULL)        \
        return SDBP_API_ERROR;

using namespace SDBPClient;

static inline uint64_t Hash(uint64_t h)
{
    return h;
}

static inline uint64_t Key(const ShardConnection* conn)
{
    return conn->GetNodeID();
}

static inline int KeyCmp(uint64_t a, uint64_t b)
{
    if (a < b)
        return -1;
    if (a > b)
        return 1;
    return 0;
}

Client::Client()
{
    controller = NULL;
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

    // set up IO thread, IOProcessor and EventLoop
    GLOBAL_MUTEX_GUARD_DECLARE();
    if (!IOProcessor::Init(MAX_IO_CONNECTION))
        return SDBP_API_ERROR;

    if (numClients == 0)
    {
        Log_Debug("Creating IOThread");
        ioThreadSignal.SetWaiting(true);
        ioThread = ThreadPool::Create(1);
        ioThread->Execute(MFUNC(Client, IOThreadFunc));
        ioThread->Start();
        ioThreadSignal.Wait();
        Log_Debug("IOThread started");
    }
    numClients++;
	Log_Debug("Number of clients: %u", numClients);
    GLOBAL_MUTEX_GUARD_UNLOCK();

    IOProcessor::BlockSignals(IOPROCESSOR_BLOCK_INTERACTIVE);

    // set default timeouts
    masterTimeout.SetCallable(MFUNC(Client, OnMasterTimeout));
    masterTimeout.SetDelay(3 * PAXOSLEASE_MAX_LEASE_TIME);
    globalTimeout.SetCallable(MFUNC(Client, OnGlobalTimeout));
    globalTimeout.SetDelay(SDBP_DEFAULT_TIMEOUT);

    // set defaults
    commandID = 0;
    result = NULL;
    batchMode = SDBP_BATCH_DEFAULT;
    batchLimit = DEFAULT_BATCH_LIMIT;
    proxy.Init();
    consistencyMode = SDBP_CONSISTENCY_STRICT;
    highestSeenPaxosID = 0;
    connectivityStatus = SDBP_NOCONNECTION;
    timeoutStatus = SDBP_SUCCESS;
    numControllerRequests = 0;
    next = prev = this;

    // create the first result object
    result = new Result;

    // wait for starting properly
    controller = Controller::GetController(this, nodec, nodev);
    if (!controller)
    {
        Shutdown();
        return SDBP_API_ERROR;
    }
    Lock();
    controller->AddClient(this);
    Unlock();

    return SDBP_SUCCESS;
}

void Client::Shutdown()
{
    if (controller == NULL)
        return;
    
    if (batchMode != SDBP_BATCH_NOAUTOSUBMIT)
        Submit();

    isShutdown.SetWaiting(true);
    if (EventLoop::IsRunning())
    {
        onClientShutdown.SetCallable(MFUNC(Client, OnClientShutdown));
        EventLoop::Add(&onClientShutdown);
        isShutdown.Wait();
    }
    EventLoop::Remove(&onClientShutdown);

    GLOBAL_MUTEX_GUARD_DECLARE();
    ASSERT(numClients != 0);
    numClients--;
    if (numClients == 0)
    {
        PooledShardConnection::ShutdownPool();
        
        Log_Debug("Stopping IOThread");
        EventLoop::Stop();
        ioThread->WaitStop();
        delete ioThread;
        ioThread = NULL;
        Log_Debug("IOThread destroyed");
    }

    IOProcessor::Shutdown();

    GLOBAL_MUTEX_GUARD_UNLOCK();

    delete result;
}

// this is always called from the IO thread with the client lock locked
void Client::OnClientShutdown()
{
    ReleaseShardConnections();
    
    EventLoop::Remove(&masterTimeout);
    EventLoop::Remove(&globalTimeout);

    if (!controller->IsShuttingDown())
        Controller::CloseController(this, controller);
    controller = NULL;
    
    shardConnections.DeleteTree();

    DeleteQuorumRequests();
    quorumRequests.Clear();
    numControllerRequests = 0;

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
    Log_Trace("timeout: %U", timeout);

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

// this must be called with the client lock locked!
ConfigState* Client::UnprotectedGetConfigState()
{
    if (!configState.hasMaster)
    {
        result->Close();
        Unlock();
        EventLoop();
        Lock();
    }

    return &configState;
}

ConfigState* Client::GetConfigState()
{
    Log_Trace();

    CLIENT_MUTEX_GUARD_DECLARE();

    if (controller == NULL)
        return NULL;
    
    if (!configState.hasMaster)
    {
        result->Close();
        CLIENT_MUTEX_GUARD_UNLOCK();
        EventLoop();
        CLIENT_MUTEX_GUARD_LOCK();
    }

    return &configState;
}

void Client::CloneConfigState(ConfigState& configState_)
{
    CLIENT_MUTEX_GUARD_DECLARE();

    if (controller == NULL)
        return;
    
    if (!configState.hasMaster)
    {
        result->Close();
        CLIENT_MUTEX_GUARD_UNLOCK();
        EventLoop();
        CLIENT_MUTEX_GUARD_LOCK();
    }

    configState_ = configState;
}

void Client::WaitConfigState()
{
    CLIENT_MUTEX_GUARD_DECLARE();

    if (controller == NULL)
        return;

    // delete configState, so EventLoop() will wait for a new one
    configState.hasMaster = false;
    result->Close();
    CLIENT_MUTEX_GUARD_UNLOCK();
    EventLoop();
    CLIENT_MUTEX_GUARD_LOCK();
}

Result* Client::GetResult()
{
	CLIENT_MUTEX_GUARD_DECLARE();

    if (controller == NULL)
        return NULL;

    Result* tmp;
    
    tmp = result;
    result = new Result;
    return tmp;
}

int Client::GetTransportStatus()
{
    VALIDATE_CONTROLLERS();
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
    VALIDATE_CONTROLLERS();
    return result->GetCommandStatus();
}

int Client::CreateDatabase(ReadBuffer& name)
{
    Request* req;

    req = new Request;
    req->CreateDatabase(controller->NextCommandID(), name);

    return ConfigRequest(req);
}

int Client::RenameDatabase(uint64_t databaseID, const ReadBuffer& name)
{
    Request* req;

    req = new Request;
    req->RenameDatabase(controller->NextCommandID(), databaseID, (ReadBuffer&) name);

    return ConfigRequest(req);
}

int Client::DeleteDatabase(uint64_t databaseID)
{
    Request* req;

    req = new Request;
    req->DeleteDatabase(controller->NextCommandID(), databaseID);

    return ConfigRequest(req);
}

int Client::CreateTable(uint64_t databaseID, uint64_t quorumID, ReadBuffer& name)
{
    Request* req;

    req = new Request;
    req->CreateTable(controller->NextCommandID(), databaseID, quorumID, name);

    return ConfigRequest(req);
}

int Client::RenameTable(uint64_t tableID, ReadBuffer& name)
{
    Request* req;

    req = new Request;
    req->RenameTable(controller->NextCommandID(), tableID, name);

    return ConfigRequest(req);
}

int Client::DeleteTable(uint64_t tableID)
{
    Request* req;

    req = new Request;
    req->DeleteTable(controller->NextCommandID(), tableID);

    return ConfigRequest(req);
}

int Client::TruncateTable(uint64_t tableID)
{
    Request* req;

    req = new Request;
    req->TruncateTable(controller->NextCommandID(), tableID);

    return ConfigRequest(req);
}

#define GET_CONFIG_STATE_OR_RETURN(...) \
    CLIENT_MUTEX_GUARD_DECLARE();       \
                                        \
    if (controller == NULL)             \
        return __VA_ARGS__;             \
                                        \
    if (!configState.hasMaster)         \
    {                                   \
        result->Close();                \
        CLIENT_MUTEX_GUARD_UNLOCK();    \
        EventLoop();                    \
        CLIENT_MUTEX_GUARD_LOCK();      \
    }                                   \
    if (!configState.hasMaster)         \
        return __VA_ARGS__;

unsigned Client::GetNumQuorums()
{
    GET_CONFIG_STATE_OR_RETURN(0);
    
    return configState.quorums.GetLength();
}

uint64_t Client::GetQuorumIDAt(unsigned n)
{
    ConfigQuorum*   quorum;
    unsigned        i;

    GET_CONFIG_STATE_OR_RETURN(0);
    
    i = 0;
    FOREACH (quorum, configState.quorums)
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
    FOREACH (quorum, configState.quorums)
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
    
    return configState.databases.GetLength();
}

uint64_t Client::GetDatabaseIDAt(unsigned n)
{
    ConfigDatabase* database;
    unsigned        i;

    GET_CONFIG_STATE_OR_RETURN(0);
    
    i = 0;
    FOREACH (database, configState.databases)
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
    FOREACH (database, configState.databases)
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
    FOREACH (database, configState.databases)
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
    FOREACH (database, configState.databases)
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
    FOREACH (database, configState.databases)
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
    
    FOREACH (table, configState.tables)
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
    Request*    req;
    Request*    it;

    if (key.GetLength() == 0)
		return SDBP_API_ERROR;
    
    VALIDATE_CONTROLLERS();
    CLIENT_MUTEX_GUARD_DECLARE();
    
    req = new Request;
    req->Get(NextCommandID(), configState.paxosID, tableID, (ReadBuffer&) key);

    // find
    it = proxy.Find(req);
    if (it)
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
    Request*    it;
    Request*    req;

    if (key.GetLength() == 0)
		return SDBP_API_ERROR;

    VALIDATE_CONTROLLERS();
    CLIENT_MUTEX_GUARD_DECLARE();
    
    req = new Request;
    req->Set(NextCommandID(), configState.paxosID,
     tableID, (ReadBuffer&) key, (ReadBuffer&) value);
    
    if (batchMode == SDBP_BATCH_NOAUTOSUBMIT &&
     proxy.GetSize() + REQUEST_SIZE(req) >= batchLimit)
    {
        delete req;
        return SDBP_API_ERROR;
    }

    it = proxy.RemoveAndAdd(req);
    if (it)
        delete it;
    
    CLIENT_MUTEX_GUARD_UNLOCK();
    
    if (batchMode == SDBP_BATCH_SINGLE)
        return Submit();
                                                    
    if (batchMode == SDBP_BATCH_DEFAULT &&
     proxy.GetSize() >= batchLimit)
        return Submit();
    
    return SDBP_SUCCESS;
}

int Client::Add(uint64_t tableID, const ReadBuffer& key, int64_t number)
{
    Request*    req;

    req = new Request;
    req->Add(NextCommandID(), configState.paxosID, tableID, (ReadBuffer&) key, number);

    return ShardRequest(req);
}

int Client::Delete(uint64_t tableID, const ReadBuffer& key)
{
    Request*    it;
    Request*    req;

    if (key.GetLength() == 0)
		return SDBP_API_ERROR;

    CLIENT_MUTEX_GUARD_DECLARE();

    req = new Request;
    req->Delete(NextCommandID(), configState.paxosID, tableID, (ReadBuffer&) key);

    if (batchMode == SDBP_BATCH_NOAUTOSUBMIT &&
     proxy.GetSize() + REQUEST_SIZE(req) >= batchLimit)
    {
        delete req;
        return SDBP_API_ERROR;
    }

    it = proxy.RemoveAndAdd(req);
    if (it)
        delete it;

    CLIENT_MUTEX_GUARD_UNLOCK();

    if (batchMode == SDBP_BATCH_SINGLE)
        return Submit();

    if (batchMode == SDBP_BATCH_DEFAULT && proxy.GetSize() >= batchLimit)
        return Submit();

    return SDBP_SUCCESS;
}

int Client::SequenceSet(uint64_t tableID, const ReadBuffer& key, const uint64_t value)
{
    Request*    req;

    req = new Request;
    req->SequenceSet(NextCommandID(), configState.paxosID, tableID, (ReadBuffer&) key, value);

    return ShardRequest(req);
}

int Client::SequenceNext(uint64_t tableID, const ReadBuffer& key)
{
    Request*    req;

    req = new Request;
    req->SequenceNext(NextCommandID(), configState.paxosID, tableID, (ReadBuffer&) key);

    return ShardRequest(req);
}

int Client::ListKeys(
 uint64_t tableID,
 const ReadBuffer& startKey, const ReadBuffer& endKey, const ReadBuffer& prefix,
 unsigned count, bool forwardDirection, bool skip)
{
    Request*    req;

    VALIDATE_CONTROLLERS();
    CLIENT_MUTEX_GUARD_DECLARE();

    req = new Request;
    req->userCount = count;
    req->skip = skip;
    req->ListKeys(NextCommandID(), configState.paxosID, tableID,
     (ReadBuffer&) startKey, (ReadBuffer&) endKey, (ReadBuffer&) prefix, count, forwardDirection);

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
 unsigned count, bool forwardDirection, bool skip)
{
    Request*    req;

    VALIDATE_CONTROLLERS();
    CLIENT_MUTEX_GUARD_DECLARE();

    req = new Request;
    req->userCount = count;
    req->skip = skip;
    req->ListKeyValues(NextCommandID(), configState.paxosID, tableID,
     (ReadBuffer&) startKey, (ReadBuffer&) endKey, (ReadBuffer&) prefix, count, forwardDirection);

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
 const ReadBuffer& startKey, const ReadBuffer& endKey, const ReadBuffer& prefix, bool forwardDirection)
{
    Request*    req;

    CLIENT_MUTEX_GUARD_DECLARE();

    req = new Request;
    req->Count(NextCommandID(), configState.paxosID,
     tableID, (ReadBuffer&) startKey, (ReadBuffer&) endKey, (ReadBuffer&) prefix, forwardDirection);
    AppendDataRequest(req);

    CLIENT_MUTEX_GUARD_UNLOCK();
    EventLoop();
    return result->GetCommandStatus();
}

int Client::Begin()
{
    Log_Trace();

    VALIDATE_CONTROLLERS();
    CLIENT_MUTEX_GUARD_DECLARE();

    result->Close();
   
    return SDBP_SUCCESS;
}

int Client::Submit()
{
    Request*    it;
    int         transportStatus;

    VALIDATE_CONTROLLERS();
    CLIENT_MUTEX_GUARD_DECLARE();

    if (proxy.GetCount() == 0)
        return SDBP_SUCCESS;
    
    Log_Trace();

    CLIENT_MUTEX_GUARD_UNLOCK();
    Begin();
    CLIENT_MUTEX_GUARD_LOCK();
    
    while ((it = proxy.Pop()) != NULL)
    {
        submittedRequests.Append(it);
        result->AppendRequest(it);
    }

    CLIENT_MUTEX_GUARD_UNLOCK();
    EventLoop();
    CLIENT_MUTEX_GUARD_LOCK();

    ClearQuorumRequests();
    submittedRequests.ClearMembers();
    
    Log_Trace("Submit returning");
    
    transportStatus = result->GetTransportStatus();
    return transportStatus;
}

int Client::Cancel()
{
    Log_Trace();

    VALIDATE_CONTROLLERS();
    CLIENT_MUTEX_GUARD_DECLARE();

    ClearRequests();
    proxy.Clear();

    result->Close();

    return SDBP_SUCCESS;
}

// =============================================================================================
//
// Client public interface ends here
//    
// =============================================================================================

int Client::ShardRequest(Request* req)
{
    Request*    itRequest;
    Request*    nextRequest;
    ReadBuffer  requestKey;

    VALIDATE_CONTROLLERS();
    CLIENT_MUTEX_GUARD_DECLARE();
	
    if (req->key.GetLength() == 0)
    {
        delete req;
        return SDBP_API_ERROR;
    }

    result->Close();

    itRequest = proxy.Find(req);
    if (itRequest)
    {
        proxy.Remove(itRequest);
        submittedRequests.Append(itRequest);
        result->AppendRequest(itRequest);
    }

    submittedRequests.Append(req);
    result->AppendRequest(req);
    CLIENT_MUTEX_GUARD_UNLOCK();
    EventLoop();
	CLIENT_MUTEX_GUARD_LOCK();
    if (result->GetCommandStatus() == SDBP_SUCCESS)
    {
        for (itRequest = result->requests.First(); itRequest != NULL; itRequest = nextRequest)
        {
            if (itRequest->commandID != req->commandID)
            {
                nextRequest = result->requests.Remove(itRequest);
                delete itRequest;
                
            }
            else
                nextRequest = result->requests.Next(itRequest);
        }
    }
    result->Begin();
    return result->GetCommandStatus();
}

int Client::ConfigRequest(Request* req)
{
    int status;

    CLIENT_MUTEX_GUARD_DECLARE();
    VALIDATE_CONTROLLERS();

    if (!configState.hasMaster)
    {
        result->Close();
        CLIENT_MUTEX_GUARD_UNLOCK();
        EventLoop();
        CLIENT_MUTEX_GUARD_LOCK();
    }

    if (proxy.GetSize() > 0)
    {
        delete req;
        return SDBP_API_ERROR;
    }

    req->client = this;

    submittedRequests.Append(req);
    numControllerRequests++;

    result->Close();
    result->AppendRequest(req);

    CLIENT_MUTEX_GUARD_UNLOCK();
    EventLoop();
    status = result->GetCommandStatus();
    return status;
}

void Client::ClearRequests()
{
    ShardConnection*    shardConnection;

    submittedRequests.Clear();
    ClearQuorumRequests();

    FOREACH (shardConnection, shardConnections)
        shardConnection->ClearRequests();
    
    if (numControllerRequests > 0)
    {
        controller->ClearRequests(this);
        numControllerRequests = 0;
    }
}

void Client::EventLoop()
{
    CLIENT_MUTEX_GUARD_DECLARE();

	if (!controller)
    {
        result->SetTransportStatus(SDBP_API_ERROR);
        return;
    }

    // avoid race conditions
    isDone.SetWaiting(true);
    if (submittedRequests.GetLength() > 0)
    {
        AssignRequestsToQuorums();
        SendQuorumRequests();
    }
    timeoutStatus = SDBP_SUCCESS;
    if (!IsDone())
    {
        Log_Trace("Not IsDone");
        EventLoop::Reset(&globalTimeout);
        if (!configState.hasMaster)
            EventLoop::Reset(&masterTimeout);
    
        CLIENT_MUTEX_GUARD_UNLOCK();
    
        //Log_Debug("%p => %U", &isDone, ThreadPool::GetThreadID());
        isDone.Wait(); // wait for IO thread to process ops
    
        CLIENT_MUTEX_GUARD_LOCK();

        EventLoop::Remove(&globalTimeout);
        EventLoop::Remove(&masterTimeout);

    }
    if (PooledShardConnection::GetPoolSize() > 0)
        ReleaseShardConnections();
    ClearRequests();
    result->SetConnectivityStatus(connectivityStatus);
    result->SetTimeoutStatus(timeoutStatus);
    result->Begin();
    CLIENT_MUTEX_GUARD_UNLOCK();
    
    Log_Trace("EventLoop() returning");
}

// This should only be called with client mutex locked!
bool Client::IsDone()
{
    if (result->GetRequestCount() == 0 && configState.paxosID != 0)
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

    if (!EventLoop::IsRunning() && EventLoop::IsStarted())
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
    return controller->GetMaster();
}

void Client::SetMaster(int64_t master_)
{
    if (configState.hasMaster)
    {
        connectivityStatus = SDBP_SUCCESS;
        EventLoop::Remove(&masterTimeout);
    }
    else
    {
        connectivityStatus = SDBP_NOMASTER;
        if (!masterTimeout.IsActive())
            EventLoop::Add(&masterTimeout);
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
    Log_Debug("Global timeout");

    CLIENT_MUTEX_GUARD_DECLARE();

    timeoutStatus = SDBP_GLOBAL_TIMEOUT;
    
    TryWake();
}

void Client::OnMasterTimeout()
{
    Log_Debug("Master timeout");

    CLIENT_MUTEX_GUARD_DECLARE();
    
    timeoutStatus = SDBP_MASTER_TIMEOUT;
    
    TryWake();
}

void Client::SetConfigState(ConfigState& configState_)
{
    configState = configState_;

    Log_Debug("configState.paxosID = %U", configState.paxosID);
    // we know the state of the system, so we can start sending requests
    if (configState.paxosID != 0)
    {
        if (configState.hasMaster)
            SetMaster(configState.masterID);
        ConfigureShardServers();
        AssignRequestsToQuorums();
        SendQuorumRequests();
    }
}

void Client::AppendDataRequest(Request* req)
{
    submittedRequests.Append(req);
    result->Close();
    result->AppendRequest(req);
}

void Client::ReassignRequest(Request* req)
{
    uint64_t        quorumID;
    ConfigQuorum*   quorum;
    ReadBuffer      key;
    
    if (configState.paxosID == 0)
        return;
    
    // handle controller requests
    if (req->IsControllerRequest())
    {
        controller->SendRequest(req);
        return;
    }

    // find quorum by key
    key.Wrap(req->key);
    if (!GetQuorumID(req->tableID, key, quorumID))
    {
        ClientResponse  response;

        response.BadSchema();
        response.commandID = req->commandID;
        result->AppendRequestResponse(&response);
        return;
    }

    // reassign the request to the new quorum
    req->quorumID = quorumID;

    quorum = configState.GetQuorum(quorumID);
    if (!req->IsReadRequest() && quorum && quorum->hasPrimary == false)
        submittedRequests.Append(req);
    else
        AddRequestToQuorum(req);
}

void Client::AssignRequestsToQuorums()
{
    Request*        it;
    Request*        next;
    RequestList     requestsCopy;
    
    if (submittedRequests.GetLength() == 0)
        return;
    
    //Log_Trace("%U", requests.First()->tableID);

    requestsCopy = submittedRequests;
    submittedRequests.ClearMembers();

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
    
    ASSERT(configState.paxosID != 0);

    table = configState.GetTable(tableID);
    if (!table)
    {
        Log_Trace("table is NULL; tableID = %U, key = %R, quorumID = %U", tableID, &key, quorumID);
        // not found
        return false;
    }
    
    FOREACH (it, table->shards)
    {
        shard = configState.GetShard(*it);
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
    if (configState.paxosID == 0)
        return;

    // check quorums
    if (!quorumRequests.Get(quorumID, qrequests))
        return;
    
    quorum = configState.GetQuorum(quorumID);
    if (!quorum)
        ASSERT_FAIL();
    
    // with consistency level set to STRICT, send requests only to primary shard server
    if (consistencyMode == SDBP_CONSISTENCY_STRICT && quorum->primaryID != conn->GetNodeID())
        return;

    if (!conn->IsConnected())
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

    FOREACH (requestNode, quorumRequests)
    {
        requestList = requestNode->Value();
        requestList->Clear();
    }
}

void Client::DeleteQuorumRequests()
{
    RequestListMap::Node*   requestNode;
    RequestList*            requestList;

    FOREACH (requestNode, quorumRequests)
    {
        requestList = requestNode->Value();
        delete requestList;
    }
}

void Client::InvalidateQuorum(uint64_t quorumID, uint64_t nodeID)
{
    ConfigQuorum*       quorum;
    ShardConnection*    shardConn;
    uint64_t*           nit;
    
    quorum = configState.GetQuorum(quorumID);
    if (!quorum)
        ASSERT_FAIL();
        
    if (quorum->hasPrimary && quorum->primaryID == nodeID)
    {
        Log_Debug("Invalidating quorum %U, nodeID %U", quorumID, nodeID);

        // invalidate shard connections
        FOREACH (nit, quorum->activeNodes)
        {
            shardConn = shardConnections.Get<uint64_t>(*nit);
            ASSERT(shardConn != NULL);
            if (nodeID == shardConn->GetNodeID())
                shardConn->ClearQuorumMembership(quorumID);
        }
    
        InvalidateQuorumRequests(quorumID);
    }
}

void Client::InvalidateQuorumRequests(uint64_t quorumID)
{
    RequestList*        qrequests;

    qrequests = NULL;
    if (!quorumRequests.Get(quorumID, qrequests))
        return;
    
    // push back requests to unselected requests' queue
    submittedRequests.PrependList(*qrequests);
}

void Client::ActivateQuorumMembership(ShardConnection* conn)
{
    ConfigQuorum*   quorum;
    uint64_t*       nit;
    uint64_t        nodeID;

    nodeID = conn->GetNodeID();
    FOREACH (quorum, configState.quorums)
    {
        FOREACH (nit, quorum->activeNodes)
        {
            if (*nit == nodeID)
                conn->SetQuorumMembership(quorum->quorumID);
        }
    }
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

    configTable = configState.GetTable(req->tableID);
    ASSERT(configTable != NULL);
    
    nextShardID = 0;
    FOREACH (itShard, configTable->shards)
    {
        configShard = configState.GetShard(*itShard);

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
    FOREACH (ssit, configState.shardServers)
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
    FOREACH (qit, configState.quorums)
    {
        Log_Trace("quorumID: %U, primary: %U", qit->quorumID, qit->hasPrimary ? qit->primaryID : 0);
        // put back sent requests to quorum requests from inactive connections
        FOREACH (nit, qit->inactiveNodes)
        {
            shardConn = shardConnections.Get(*nit);
            if (shardConn != NULL)
                shardConn->ReassignSentRequests();
        }
        
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

void Client::ReleaseShardConnections()
{
    ShardConnection*    shardConnection;

    FOREACH (shardConnection, shardConnections)
        shardConnection->ReleaseConnection();
}

void Client::OnControllerConnected(ControllerConnection* conn)
{
    UNUSED(conn);
    
    if (connectivityStatus == SDBP_NOCONNECTION)
        connectivityStatus = SDBP_NOMASTER;
}

void Client::OnControllerDisconnected(ControllerConnection* conn)
{
    UNUSED(conn);
    
    //if (master == (int64_t) conn->GetNodeID())
    //    SetMaster(-1, conn->GetNodeID());
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
    
    request = result->GetRequestCursor();

    // compute proxied result into proxyKeys/proxyValues
    FOREACH(itProxyRequest, proxy)
    {
        if (itProxyRequest->tableID < request->tableID)
            continue;
        if (itProxyRequest->tableID > request->tableID)
            break;
        // tableID matches, look at keys
        if (request->endKey.GetLength() > 0)
        {
            cmp = Buffer::Cmp(itProxyRequest->key, request->endKey);
            if ((request->forwardDirection && cmp >= 0) || (!request->forwardDirection && cmp <= 0))
                break;
        }
        
        if (itProxyRequest->key.BeginsWith(request->prefix))
        {
            cmp = Buffer::Cmp(itProxyRequest->key, request->key /*startKey*/);
            if (cmp == 0 && request->skip)
                continue;
            else if ((request->key.GetLength() == 0) || ((request->forwardDirection && cmp >= 0) || (!request->forwardDirection && cmp <= 0)))
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

    // CopyResult
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
        else if ((request->forwardDirection && cmp < 0) || (!request->forwardDirection && cmp > 0))
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
    FOREACH_FIRST(itResponse, request->responses)
    {
        delete *itResponse;
        request->responses.Remove(itResponse);
    }

    request->responses.Append(response);
}

uint64_t Client::NumProxiedDeletes(Request* request)
{
    int         cmp;
    uint64_t    count;
    Request*    itProxyRequest;
    
    count = 0;
    FOREACH(itProxyRequest, proxy)
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
    //Log_Debug("TryWake: %U => %p", ThreadPool::GetThreadID(), &isDone);
    LockGuard<Signal>   signalGuard(isDone);

    if (!isDone.IsWaiting())
        return;

    if (IsDone())
        isDone.UnprotectedWake();
}

bool Client::IsShuttingDown()
{
    LockGuard<Signal>   signalGuard(isShutdown);

    return isShutdown.IsWaiting();
}

void Client::IOThreadFunc()
{
    long    sleep;

    Log_SetTimestamping(true);
    Log_SetThreadedOutput(true);
    Log_Debug("IOThreadFunc started: %U", ThreadPool::GetThreadID());

    EventLoop::Start();
    EventLoop::UpdateTime();
    ioThreadSignal.Wake();

    while (EventLoop::IsRunning())
    {
        sleep = EventLoop::RunTimers();
    
        //if (sleep < 0 || sleep > SLEEP_MSEC)
        if (sleep < 0)
            sleep = SLEEP_MSEC;
    
        if (!IOProcessor::Poll(sleep))
        {
            //STOP_FAIL(0, "Ctrl-C was pressed");
            break;
        }
    }

    EventLoop::Stop();
    Log_Debug("EventLoop stopped");

    // TODO: HACK
    // When IOProcessor is terminated (e.g. with Ctrl-C from console) no network/timer
    // event will be ever delivered. That means clients waiting for signals would be stuck
    // forever. Therefore we wake up all clients here.
    Controller::TerminateClients();

    Log_Debug("IOThreadFunc finished: %U", ThreadPool::GetThreadID());
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

Mutex& Client::GetGlobalMutex()
{
    return globalMutex;
}
