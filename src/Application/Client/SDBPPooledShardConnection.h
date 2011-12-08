#ifndef SDBPPOOLEDSHARDCONNECTION_H
#define SDBPPOOLEDSHARDCONNECTION_H

#include "System/Containers/InTreeMap.h"
#include "Framework/Messaging/MessageConnection.h"
#include "Application/SDBP/SDBPRequestMessage.h"

namespace SDBPClient
{

class ShardConnection;

/*
===============================================================================================

 SDBPClient::PooledShardConnection

===============================================================================================
*/

class PooledShardConnection : MessageConnection
{
    typedef InTreeNode<PooledShardConnection> TreeNode;

public:    
    ~PooledShardConnection();

    static PooledShardConnection*       GetConnection(ShardConnection* conn);
    static void                         ReleaseConnection(PooledShardConnection* conn);
    static void                         Cleanup();
    static void                         SetPoolSize(unsigned poolSize);
    static unsigned                     GetPoolSize();
    static void                         ShutdownPool();
    
    const Buffer&                       GetName() const;

    void                                Connect();
    void                                Flush();
    bool                                SendRequest(SDBPRequestMessage& msg);
    bool                                IsWritePending();
    bool                                IsConnected();

    // MessageConnection interface
    virtual bool                        OnMessage(ReadBuffer& msg);
    virtual void                        OnWrite();
    virtual void                        OnConnect();
    virtual void                        OnClose();

    PooledShardConnection*              prev;
    PooledShardConnection*              next;
    
private:
    PooledShardConnection(Endpoint& endpoint);

    ShardConnection*                    conn;
    Buffer                              name;
    Endpoint                            endpoint;
    uint64_t                            lastUsed;
};

};  // namespace

#endif