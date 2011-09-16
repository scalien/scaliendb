#ifndef SDBPCONTROLLER_H
#define SDBPCONTROLLER_H

#include "System/Containers/InList.h"
#include "System/Containers/InTreeMap.h"
#include "System/Threading/Mutex.h"
#include "System/Events/Timer.h"
#include "SDBPClientRequest.h"

namespace SDBPClient
{

class Client;
class ControllerConnection;

class Controller
{
    friend class ControllerPool;
    typedef InList<Client> ClientList;
    typedef InList<Request> RequestList;
    typedef InTreeNode<Controller> TreeNode;

public:
    static Controller*      GetController(Client* client, int nodec, const char* argv[]);
    static void             CloseController(Client* client, Controller* conn);

    // call this when IOThread terminates
    static void             TerminateClients();
    
    // client interface
    void                    AddClient(Client* client);

    void                    ClearRequests(Client* client);
    void                    SendRequest(Request* request);

    void                    GetConfigState(ConfigState& configState, bool useLock);
    bool                    HasMaster();
    bool                    IsMaster(uint64_t nodeID);
    int64_t                 GetMaster();
    uint64_t                NextCommandID();
    const Buffer&           GetName() const;
    bool                    IsShuttingDown();

    // connection interface
    void                    OnConnected(ControllerConnection* conn);
    void                    OnDisconnected(ControllerConnection* conn);
    void                    OnRequestResponse(Request* request, ClientResponse* response);
    void                    OnNoService(ControllerConnection* conn);
    void                    SetConfigState(ControllerConnection* conn, ConfigState* configState);

    TreeNode                treeNode;

private:
    // pool interface
    Controller(int nodec, const char* nodev[]);
    ~Controller();

    void                    Shutdown();

    void                    RemoveClient(Client* client);
    unsigned                GetNumClients();
    int                     GetNumControllers();

    // YieldTimer callbacks
    void                    OnConfigStateChanged();
    void                    OnSendRequest();

    ControllerConnection**  controllerConnections;
    int                     numControllers;
    ClientList              clients;
    RequestList             requests;
    uint32_t                nextCommandID;
    Buffer                  name;
    ConfigState             configState;
    Mutex                   mutex;
    YieldTimer              onSendRequest;
    bool                    isShuttingDown;
};

};

#endif
