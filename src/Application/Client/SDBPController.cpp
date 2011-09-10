#include "SDBPController.h"
#include "SDBPControllerConnection.h"
#include "System/Threading/Atomic.h"
#include "System/Containers/InTreeMap.h"

using namespace SDBPClient;

static InTreeMap<Controller>    controllers;

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

    for (int i = 0; i < nodec; i++)
    {
        Endpoint    endpoint;
        
        if (!endpoint.Set(nodev[i], true))
            return NULL;

        // FIXME: this logic is duplicated in Controller
        controllerName.Appendf("/%s", endpoint.ToString());
    }        

    controller = controllers.Get(controllerName);
    if (controller)
    {
        controller->AddClient(client);
        return controller;
    }

    controller = new Controller(nodec, nodev);
    if (controller->GetNumControllers() == 0)
    {
        delete controller;
        return NULL;
    }

    controllers.Insert<const Buffer&>(controller);
    controller->AddClient(client);

    return controller;
}

void Controller::CloseController(Client* client, Controller* controller)
{
    controller->RemoveClient(client);
    if (controller->GetNumClients() == 0)
    {
        controllers.Remove(controller);
        delete controller;
    }
}

void Controller::WakeClients()
{
    Controller*     controller;

    MutexGuard      guard(Client::GetGlobalMutex());

    FOREACH (controller, controllers)
    {
        controller->OnConfigStateChanged();
    }
}

void Controller::ClearRequests(Client* client)
{
    requests.Clear();

    for (int i = 0; i < numControllers; i++)
    {
        controllerConnections[i]->ClearRequests(client);
    }
}

void Controller::SendRequest(Request* request)
{
    if (HasMaster())
        controllerConnections[configState.masterID]->SendRequest(request);
    else
        requests.Append(request);
}

ConfigState* Controller::GetConfigState()
{
    return &configState;
}

bool Controller::HasMaster()
{
    return configState.hasMaster;
}

uint64_t Controller::NextCommandID()
{
    return AtomicIncrement64(nextCommandID);
}

const Buffer& Controller::GetName() const
{
    return name;
}

void Controller::OnConnected(ControllerConnection* conn)
{
    UNUSED(conn);
    // TODO:
}

void Controller::OnDisconnected(ControllerConnection* conn)
{
    if (configState.hasMaster && configState.masterID == conn->GetNodeID())
    {
        configState.hasMaster = false;
        configState.masterID = -1;
        OnConfigStateChanged();
    }
}

void Controller::OnRequestResponse(Request* request, ClientResponse* resp)
{
    Client*     client;

    if (request && request->client)
    {
        client = request->client;
        
        // TODO: YieldTimer
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

void Controller::SetConfigState(ControllerConnection* conn, ConfigState* configState_)
{
    uint64_t    nodeID;
    Log_Debug("configState");
    nodeID = conn->GetNodeID();

    if (!configState.hasMaster || configState.masterID == nodeID)
    {
        mutex.Lock();
        configState = *configState_;
        mutex.Unlock();

        // Avoid potential deadlock by calling OnConfigStateChanged from YieldTimer
        //OnConfigStateChanged();
        if (!onConfigStateChanged.IsActive())
            EventLoop::Add(&onConfigStateChanged);
    }
}

void Controller::OnConfigStateChanged()
{
    Client*     client;

    MutexGuard  mutexGuard(mutex);

    FOREACH (client, clients)
    {
        client->Lock();
        
        client->SetConfigState(&configState);

        if (configState.hasMaster)
            client->SetMaster(configState.masterID);
        else
            client->SetMaster(-1);
        client->TryWake();

        client->Unlock();
    }
}

Controller::Controller(int nodec, const char* nodev[])
{
    nextCommandID = 0;
    onConfigStateChanged.SetCallable(MFUNC(Controller, OnConfigStateChanged));

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

    if (onConfigStateChanged.IsActive())
        EventLoop::Remove(&onConfigStateChanged);

    Log_Debug("Controller Shutdown finished");
}

void Controller::AddClient(Client* client)
{
    mutex.Lock();
    clients.Append(client);

    if (configState.hasMaster)
    {
        client->Lock();
        client->SetMaster(configState.masterID);
        client->SetConfigState(&configState);
        client->TryWake();
        client->Unlock();
    }

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
