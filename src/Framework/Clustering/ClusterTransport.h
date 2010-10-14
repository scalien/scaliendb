#ifndef CLUSTERTRANSPORT_H
#define CLUSTERTRANSPORT_H

#include "System/Containers/List.h"
#include "Framework/Messaging/Message.h"
#include "ClusterServer.h"

class WriteReadyness;

/*
===============================================================================================

 ClusterTransport

===============================================================================================
*/

class ClusterTransport
{
public:
    virtual ~ClusterTransport() {}
    
    void                        Init(Endpoint& endpoint);
    
    void                        SetSelfNodeID(uint64_t nodeID);
    
    bool                        IsAwaitingNodeID();
    uint64_t                    GetSelfNodeID();
    Endpoint&                   GetSelfEndpoint();

    void                        AddNode(uint64_t nodeID, Endpoint& endpoint);
    bool                        SetConnectionNodeID(Endpoint& endpoint, uint64_t nodeID);
    
    void                        SendMessage(uint64_t nodeID, Buffer& prefix, Message& msg);
    void                        SendPriorityMessage(uint64_t nodeID, Buffer& prefix, Message& msg);
    
    void                        DropConnection(uint64_t nodeID);
    void                        DropConnection(Endpoint endpoint);
    
    virtual void                OnConnectionReady(uint64_t nodeID, Endpoint endpoint)           = 0;
    virtual bool                OnAwaitingNodeID(Endpoint endpoint)                             = 0;
    virtual void                OnMessage(uint64_t nodeID, ReadBuffer msg)                      = 0;

    bool                        GetNextWaiting(Endpoint& endpoint);
    
    bool                        IsConnected(uint64_t nodeID);

    void                        RegisterWriteReadyness(uint64_t nodeID, Callable callable);
    void                        UnregisterWriteReadyness(uint64_t nodeID, Callable callable);

private:
    // for ClusterConnection:
    void                        AddConnection(ClusterConnection* conn);
    ClusterConnection*          GetConnection(uint64_t nodeID);
    ClusterConnection*          GetConnection(Endpoint& endpoint);
    void                        DeleteConnection(ClusterConnection* conn);
    void                        ReconnectAll();
    void                        OnWriteReadyness(ClusterConnection* conn);

    bool                        awaitingNodeID;
    uint64_t                    nodeID;
    Endpoint                    endpoint;
    Buffer                      msgBuffer;
    ClusterServer               server;
    InList<ClusterConnection>   conns;
    InList<WriteReadyness>      writeReadynessList;

    friend class ClusterConnection;
};

/*
===============================================================================================

 WriteReadyness

===============================================================================================
*/

class WriteReadyness
{
public:
    WriteReadyness()
    {
        prev = next = this;
    }

    uint64_t            nodeID;
    Callable            callable;
    WriteReadyness*     prev;
    WriteReadyness*     next;
};


#endif
