#include "SDBPClient.h"
#include "SDBPControllerConnection.h"
#include "SDBPShardConnection.h"
#include "SDBPClientConsts.h"
#include "System/IO/IOProcessor.h"
#include "Framework/Replication/PaxosLease/PaxosLease.h"
#include "Application/Common/ClientRequest.h"
#include "Application/Common/ClientResponse.h"

#define MAX_SERVER_NUM  256

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
    globalTimeout.SetCallable(MFUNC(Client, OnGlobalTimeout));
    masterTimeout.SetCallable(MFUNC(Client, OnMasterTimeout));
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

    // TODO: find out the optimal size of MAX_SERVER_NUM
    if (!IOProcessor::Init(nodec + MAX_SERVER_NUM))
        return SDBP_API_ERROR;

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
    
    return SDBP_SUCCESS;
}

void Client::Shutdown()
{
    if (!controllerConnections)
        return;
    
    for (int i = 0; i < numControllers; i++)
        delete controllerConnections[i];
    
    delete controllerConnections;
    controllerConnections = NULL;
    
    delete result;
    
    EventLoop::Remove(&masterTimeout);
    EventLoop::Remove(&globalTimeout);
    IOProcessor::Shutdown();
}

void Client::SetGlobalTimeout(uint64_t timeout)
{
    globalTimeout.SetDelay(timeout);
}

void Client::SetMasterTimeout(uint64_t timeout)
{
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
    
    if (configState == NULL)
    {
        result->Close();
        EventLoop();
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
    
    assert(configState != NULL);
    table = configState->GetTable(databaseID, name);
    if (!table)
        return SDBP_BADSCHEMA;
    
    tableID = table->tableID;
    return SDBP_SUCCESS;
}

int Client::Get(uint64_t databaseID, uint64_t tableID, ReadBuffer& key)
{
    Request*    req;
    
    // TODO validations
    
    req = new Request;
    req->Get(NextCommandID(), databaseID, tableID, key);
    
    result->Close();
    result->AppendRequest(req);
    requests.Append(req);
    
    EventLoop();
    return result->CommandStatus();
}

int Client::Set(uint64_t databaseID, uint64_t tableID, ReadBuffer& key, ReadBuffer& value)
{
    Request*    req;
    
    // TODO validations
    
    req = new Request;
    req->Set(NextCommandID(), databaseID, tableID, key, value);
    
    result->Close();
    result->AppendRequest(req);
    requests.Append(req);
    
    EventLoop();
    return result->CommandStatus(); 
}

int Client::Delete(uint64_t databaseID, uint64_t tableID, ReadBuffer& key)
{
    Request*    req;
    
    // TODO validations
    
    req = new Request;
    req->Delete(NextCommandID(), databaseID, tableID, key);
    
    result->Close();
    result->AppendRequest(req);
    requests.Append(req);
    
    EventLoop();
    return result->CommandStatus();
}

void Client::EventLoop()
{
    if (!controllerConnections)
    {
        result->transportStatus = SDBP_API_ERROR;
        return;
    }

    AssignRequestsToQuorums();
    SendQuorumRequests();
    
    EventLoop::UpdateTime();
    EventLoop::Reset(&globalTimeout);
    EventLoop::Reset(&masterTimeout);
    timeoutStatus = SDBP_SUCCESS;
    
    while (!IsDone())
    {
        if (!EventLoop::RunOnce())
            break;
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
    Log_Trace("known master: %d, set master: %d, nodeID: %d", master, master_, nodeID);
    
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
    timeoutStatus = SDBP_GLOBAL_TIMEOUT;
}

void Client::OnMasterTimeout()
{
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
    }
}

void Client::ReassignRequest(Request* req)
{
    uint64_t        quorumID;
    ConfigQuorum*   quorum;
    ReadBuffer      key;
    
    if (!configState)
        return;
    
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
    
    requestsCopy = requests;
    requests.ClearMembers();

    for (it = requestsCopy.First(); it != NULL; it = next)
    {
        next = requestsCopy.Remove(it);
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
        if (qrequests->GetLength() > 0)
        {   
            req = qrequests->First();
            qrequests->Remove(req);
            conn->SendRequest(req);
        }
        else
            conn->SendSubmit();
    }
}

void Client::SendQuorumRequests()
{
    ShardConnection*    conn;
    uint64_t*           qit;
    RequestList*        qrequests;
    
    for (conn = shardConnections.First(); conn != NULL; conn = shardConnections.Next(conn))
    {
        if (conn->IsWritePending())
            continue;

        SortedList<uint64_t>& quorums = conn->GetQuorumList();
        for (qit = quorums.First(); qit != NULL; qit = quorums.Next(qit))
        {
            if (!quorumRequests.Get(*qit, qrequests))
                continue;
            
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
    
    // TODO: removal of node
    shardServers = &configState->shardServers;
    for (ssit = shardServers->First(); ssit != NULL; ssit = shardServers->Next(ssit))
    {
        shardConn = shardConnections.Get<uint64_t>(ssit->nodeID);
        if (shardConn == NULL)
        {
            shardConn = new ShardConnection(this, ssit->nodeID, ssit->endpoint);
            shardConnections.Insert(shardConn);
        }
        else
        {
            assert(ssit->endpoint == shardConn->GetEndpoint());
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
        SetMaster(conn->GetNodeID(), -1);
}
