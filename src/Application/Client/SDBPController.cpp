#include "SDBPController.h"
#include "SDBPControllerConnection.h"
#include "System/Threading/Atomic.h"
#include "System/Containers/InTreeMap.h"

using namespace SDBPClient;

static InTreeMap<Controller>    controllers;
static Mutex                    globalMutex;

static inline const Buffer& Key(const Controller* controller)
{
    return controller->GetName();
}

static inline int KeyCmp(const Buffer& a, const Buffer& b)
{
    return Buffer::Cmp(a, b);
}

Controller* Controller::GetController(Client* client, int nodec, const char* nodev[])
{
    Controller*     controller;
    Buffer          controllerName;

    // create a unique controller name from the given endpoints
    for (int i = 0; i < nodec; i++)
    {
        Endpoint    endpoint;
        
        if (!endpoint.Set(nodev[i], true))
            return NULL;

        controllerName.Appendf("/%s", endpoint.ToString());
    }        

    // The number of clients acts as a reference counter on a Controller
    // object.  When either the number of clients is changed or the global
    // treemap holding the controllers is changed, the globalMutex should
    // be locked.
    MutexGuard      guard(globalMutex);

    controller = controllers.Get(controllerName);
    if (controller)
    {
        // TODO:
        UNUSED(client);
        //controller->AddClient(client);
        return controller;
    }

    controller = new Controller(nodec, nodev);
    if (controller->GetNumControllers() == 0)
    {
        delete controller;
        return NULL;
    }

    controllers.Insert<const Buffer&>(controller);
    //controller->AddClient(client);

    return controller;
}

void Controller::CloseController(Client* client, Controller* controller)
{
    MutexGuard      guard(globalMutex);

    controller->RemoveClient(client);
    if (controller->GetNumClients() == 0)
    {
        controllers.Remove(controller);
        delete controller;
    }
}

void Controller::TerminateClients()
{
    Controller*     controller;

    Log_Debug("Controller::TerminateClients");

    MutexGuard      guard(globalMutex);

    FOREACH_FIRST (controller, controllers)
    {
        guard.Unlock();
        controller->isShuttingDown = true;
        controller->OnConfigStateChanged();
        controllers.Remove(controller);
        delete controller;
        guard.Lock();
    }
}

void Controller::ClearRequests(Client* client)
{
    MutexGuard  mutexGuard(mutex);

    requests.Clear();

    for (int i = 0; i < numControllers; i++)
    {
        controllerConnections[i]->ClearRequests(client);
    }
}

void Controller::SendRequest(Request* request)
{
    MutexGuard  mutexGuard(mutex);

    requests.Append(request);

    if (!onSendRequest.IsActive())
        EventLoop::Add(&onSendRequest);
}

void Controller::GetConfigState(ConfigState& configState_, bool useLock)
{
    if (useLock)
        mutex.Lock();

    configState_ = configState;

    if (useLock)
        mutex.Unlock();
}

bool Controller::HasMaster()
{
    return configState.hasMaster;
}

bool Controller::IsMaster(uint64_t nodeID)
{
    return (configState.hasMaster && configState.masterID == nodeID);
}

int64_t Controller::GetMaster()
{
    // FIXME: this is not thread-safe!
    if (!configState.hasMaster)
        return -1;
    return configState.masterID;
}

uint64_t Controller::NextCommandID()
{
    // TODO: workaround for compatibility with Windows XP as 64bit atomic operations are not supported on it
    //return AtomicIncrement64(nextCommandID);

    return (uint64_t) AtomicIncrement32(nextCommandID);
}

const Buffer& Controller::GetName() const
{
    return name;
}

bool Controller::IsShuttingDown()
{
    return isShuttingDown;
}

void Controller::OnConnected(ControllerConnection* conn)
{
    UNUSED(conn);
    // TODO:
}

void Controller::OnDisconnected(ControllerConnection* conn)
{
    if (IsMaster(conn->GetNodeID()))
    {
        configState.hasMaster = false;
        configState.masterID = -1;
        configState.paxosID = 0;
        OnConfigStateChanged();
    }
}

void Controller::OnRequestResponse(Request* request, ClientResponse* resp)
{
    Client*     client;

    if (request && request->client)
    {
        client = request->client;

        // TODO: if client is waiting for isDone signal then no locking is necessary
        client->Lock();
        client->result->AppendRequestResponse(resp);
        client->TryWake();
        client->Unlock();
    }
}

void Controller::OnNoService(ControllerConnection* conn)
{
    // this has the same effect as disconnecting
    OnDisconnected(conn);
}

bool IsConfigQuorumPrimaryChanged(ConfigState* configState, ConfigState* oldConfigState)
{
    ConfigQuorum*   quorum;
    ConfigQuorum*   oldQuorum;

    for (quorum = configState->quorums.First(), oldQuorum = oldConfigState->quorums.First();
      quorum && oldQuorum;
      quorum = configState->quorums.Next(quorum), oldQuorum = oldConfigState->quorums.Next(oldQuorum))
    {
        // we assume that only the primary changed not the quorum configuration
        ASSERT(quorum->quorumID == oldQuorum->quorumID);
        if (quorum->primaryID != oldQuorum->primaryID)
            return true;
    }

    return false;
}

void Controller::SetConfigState(ControllerConnection* conn, ConfigState* configState_)
{
    uint64_t    nodeID;
    bool        updateClients;
    
    nodeID = conn->GetNodeID();

    updateClients = false;
    if (!configState.hasMaster || configState.masterID == nodeID)
    {
        if (!configState.hasMaster || configState.masterID != nodeID)
            Log_Debug("Node %U became the master", nodeID);

        // optimization: update clients only when configState.paxosID is changed
        // or quorum primary changed
        if (configState_->paxosID > configState.paxosID)
            updateClients = true;
        if (!updateClients && IsConfigQuorumPrimaryChanged(configState_, &configState))
            updateClients = true;

        mutex.Lock();
        configState_->Transfer(configState);
        mutex.Unlock();

        if (updateClients)
            OnConfigStateChanged();
    }
}

void Controller::OnConfigStateChanged()
{
    Client*     client;

    Log_Debug("OnConfigStateChanged, shutdown = %s", isShuttingDown ? "true" : "false");

    MutexGuard  mutexGuard(mutex);

    FOREACH (client, clients)
    {
        client->Lock();
        
        client->SetConfigState(configState);
        client->TryWake();
        
        if (isShuttingDown)
            client->OnClientShutdown();

        client->Unlock();
    }
}

void Controller::OnSendRequest()
{
    Request*    request;

    if (!HasMaster())
        return;

    MutexGuard  guard(mutex);

    FOREACH_FIRST (request, requests)
    {
        requests.Remove(request);
        controllerConnections[configState.masterID]->SendRequest(request);
    }
}

Controller::Controller(int nodec, const char* nodev[])
{
    isShuttingDown = false;
    nextCommandID = 0;
    onSendRequest.SetCallable(MFUNC(Controller, OnSendRequest));

    numControllers = 0;
    controllerConnections = new ControllerConnection*[nodec];
    for (int i = 0; i < nodec; i++)
    {
        Endpoint    endpoint;
        
        if (!endpoint.Set(nodev[i], true))
            break;
        
        controllerConnections[i] = new ControllerConnection(this, (uint64_t) i, endpoint);
        numControllers = i + 1;
        name.Appendf("/%s", endpoint.ToString());
    }

    if (numControllers != nodec)
        Shutdown();
}

Controller::~Controller()
{
    Shutdown();
}

void Controller::Shutdown()
{
    name.Clear();
    for (int i = 0; i < numControllers; i++)
    {
        controllerConnections[i]->Close();
        delete controllerConnections[i];
    }
    numControllers = 0;

    delete[] controllerConnections;
    controllerConnections = NULL;

    EventLoop::Remove(&onSendRequest);

    Log_Debug("Controller Shutdown finished");
}

void Controller::AddClient(Client* client)
{
    mutex.Lock();
    clients.Append(client);

    if (configState.hasMaster)
        client->SetConfigState(configState);

    mutex.Unlock();
}

void Controller::RemoveClient(Client* client)
{
    mutex.Lock();
    clients.Remove(client);
    mutex.Unlock();
}

unsigned Controller::GetNumClients()
{
    unsigned    numClients;

    mutex.Lock();
    numClients = clients.GetLength();
    mutex.Unlock();
    
    return numClients;
}

int Controller::GetNumControllers()
{
    return numControllers;
}
