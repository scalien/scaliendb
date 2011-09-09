#ifndef SDBPCONTROLLERCONNECTION_H
#define SDBPCONTROLLERCONNECTION_H

#include "System/Containers/List.h"
#include "System/Containers/InTreeMap.h"
#include "System/Platform.h"
#include "Framework/Messaging/MessageConnection.h"
#include "Application/Common/ClientResponse.h"
#include "SDBPClient.h"

namespace SDBPClient
{

class Controller;

/*
===============================================================================================

 SDBPClient::ControllerConnection

===============================================================================================
*/

class ControllerConnection : public MessageConnection
{
    friend class Controller;
    typedef InList<Request> RequestList;
public:
    // client interface
    void            ClearRequests(Client* client);
    void            SendRequest(Request* request);
    uint64_t        GetNodeID() const;

    // internal timeout callback
    void            OnGetConfigStateTimeout();  

    // MessageConnection interface
    virtual bool    OnMessage(ReadBuffer& msg);
    virtual void    OnWrite();
    virtual void    OnConnect();
    virtual void    OnClose();

private:
    ControllerConnection(Controller* controller, uint64_t nodeID, Endpoint& endpoint);
    ~ControllerConnection();

    bool            ProcessResponse(ClientResponse* resp);
    bool            ProcessGetConfigState(ClientResponse* resp);
    bool            ProcessCommandResponse(ClientResponse* resp);
    Request*        RemoveRequest(uint64_t commandID);
    void            Connect();
    void            SendGetConfigState();

    Controller*     controller;
    uint64_t        nodeID;
    Endpoint        endpoint;
    ConfigState     configState;
    uint64_t        getConfigStateTime;
    Countdown       getConfigStateTimeout;
    bool            getConfigStateSent;
    RequestList     requests;
    Mutex           mutex;
};

}; // namespace

#endif
