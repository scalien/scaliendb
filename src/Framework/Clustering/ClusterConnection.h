#ifndef CLUSTERCONNECTION_H
#define CLUSTERCONNECTION_H

#include "System/Stopwatch.h"
#include "System/Events/EventLoop.h"
#include "System/Buffers/Buffer.h"
#include "Framework/Messaging/MessageConnection.h"
#include "Framework/Messaging/Message.h"

class ClusterTransport;     // forward

/*
===============================================================================================

 ClusterConnection

===============================================================================================
*/

class ClusterConnection : public MessageConnection
{
public:
    enum Progress
    {
        INCOMING,           // connection established, nodeID not received, endpoint == unknown
        AWAITING_NODEID,    // connection established, other side has no nodeID, endpoint is known
        OUTGOING,           // connecting in progress, nodeID not sent
        READY               // connection established, other side's nodeID known
    };

    void                InitConnected(bool startRead = true);
    void                SetTransport(ClusterTransport* transport);

    void                SetNodeID(uint64_t nodeID);
    void                SetEndpoint(Endpoint& endpoint);
    void                SetProgress(Progress progress);
    
    uint64_t            GetNodeID();
    Endpoint            GetEndpoint();
    Progress            GetProgress();

    void                Connect();
    void                OnConnect();
    void                OnClose();

    // MessageConnection interface
    // Returns whether the connection was closed and deleted
    virtual bool        OnMessage(ReadBuffer& msg);

    virtual void        OnWriteReadyness();

private:
    Progress            progress;
    uint64_t            nodeID;
    ClusterTransport*   transport;
    
    friend class ClusterTransport;
};

#endif
