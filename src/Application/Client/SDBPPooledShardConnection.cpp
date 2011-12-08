#include "SDBPPooledShardConnection.h"
#include "SDBPShardConnection.h"
#include "System/Containers/HashMap.h"
#include "System/Threading/Mutex.h"

using namespace SDBPClient;

#define CLEANUP_TIMEOUT                     (15*1000)

/*
===============================================================================================

 SDBPClient::PooledShardConnectionList

 Used for holding PooledShardConnections in a map, keyed by the endpoint of the connection.

===============================================================================================
*/

class PooledShardConnectionList
{
public:
    Buffer                                  name;
    InList<PooledShardConnection>           list;
    InTreeNode<PooledShardConnectionList>   treeNode;
};

/*
===============================================================================================

 Globals

===============================================================================================
*/

typedef InTreeMap<PooledShardConnectionList> ConnectionListMap;

static ConnectionListMap    connections;
static Mutex                globalMutex;
static Countdown            cleanupTimer;
static unsigned             poolSize = 0;
static unsigned             maxPoolSize = 0;

/*
===============================================================================================

 Helpers

===============================================================================================
*/

static inline const Buffer& Key(const PooledShardConnectionList* conn)
{
    return conn->name;
}

static inline int KeyCmp(const Buffer& a, const Buffer& b)
{
    return Buffer::Cmp(a, b);
}

/*
===============================================================================================

 SDBPClient::PooledShardConnection implementation.

===============================================================================================
*/

PooledShardConnection* PooledShardConnection::GetConnection(ShardConnection* shardConn)
{
    PooledShardConnection*      conn;
    PooledShardConnectionList*  connList;
    Buffer                      name;
    
    MutexGuard                  guard(globalMutex);

    name.Write(shardConn->GetEndpoint().ToReadBuffer());
    connList = connections.Get(name);

    // create new list if not exists by the name
    if (connList == NULL)
    {
        connList = new PooledShardConnectionList();
        connList->name.Write(name);
        connections.Insert<const Buffer&>(connList);
    }
    
    if (connList->list.GetLength() == 0)
    {
        // create new connection and start connecting
        conn = new PooledShardConnection(shardConn->GetEndpoint());
        Log_Debug("New connection, nodeID: %U, endpoint: %s", shardConn->GetNodeID(), shardConn->GetEndpoint().ToString());
    }
    else
    {
        // acquire pooled connection
        conn = connList->list.Pop();
        poolSize -= 1;
        Log_Debug("Pooled connection, nodeID: %U, endpoint: %s", shardConn->GetNodeID(), shardConn->GetEndpoint().ToString());
    }

    conn->conn = shardConn;

    return conn;
}

void PooledShardConnection::ReleaseConnection(PooledShardConnection* conn)
{
    PooledShardConnectionList*  connList;

    MutexGuard      guard(globalMutex);

    Log_Debug("Connection released, nodeID: %U, endpoint: %s", conn->conn->GetNodeID(), conn->conn->GetEndpoint().ToString());

    conn->conn = NULL;
    conn->lastUsed = EventLoop::Now();
    
    // no connection pooling
    if (maxPoolSize == 0)
    {
        delete conn;
        return;
    }
    
    // put back the connection to the pool, the cleanup mechanism will take care of idle connections
    connList = connections.Get(conn->GetName());
    ASSERT(connList != NULL);
    connList->list.Prepend(conn);
    poolSize += 1;
}

void PooledShardConnection::Cleanup()
{
    PooledShardConnectionList*      connList;
    PooledShardConnection*          conn;
    PooledShardConnection*          next;
    InList<PooledShardConnection>   deleteList;
    uint64_t                        now;

    MutexGuard      guard(globalMutex);

    if (poolSize <= maxPoolSize)
        return;

    // find all connections that are idle for a certain time
    now = EventLoop::Now();
    FOREACH (connList, connections)
    {
        for (conn = connList->list.First(); conn; conn = next)
        {
            if (now - conn->lastUsed > CLEANUP_TIMEOUT)
            {
                next = connList->list.Remove(conn);
                deleteList.Append(conn);
            }
            else
                next = connList->list.Next(conn);
        }
    }

    guard.Unlock();

    // do the memory deallocation outside the mutex
    deleteList.DeleteList();
}

void PooledShardConnection::SetPoolSize(unsigned poolSize_)
{
    MutexGuard      guard(globalMutex);

    maxPoolSize = poolSize_;
    if (maxPoolSize > 0)
    {
        // start the cleanup timer
        if (!cleanupTimer.IsActive())
        {
            cleanupTimer.SetDelay(CLEANUP_TIMEOUT);
            cleanupTimer.SetCallable(CFunc(PooledShardConnection::Cleanup));
            EventLoop::Add(&cleanupTimer);
        }
    }
    else
    {
        // remove the cleanup timer
        if (cleanupTimer.IsActive())
        {
            EventLoop::Remove(&cleanupTimer);
        }
    }
}

unsigned PooledShardConnection::GetPoolSize()
{
    return maxPoolSize;
}

void PooledShardConnection::ShutdownPool()
{
    PooledShardConnectionList*  connList;

    MutexGuard      guard(globalMutex);

    FOREACH (connList, connections)
    {
        connList->list.DeleteList();
    }

    connections.DeleteTree();
}

const Buffer& PooledShardConnection::GetName() const
{
    return name;
}

void PooledShardConnection::Connect()
{
    MessageConnection::Connect(endpoint);
}

void PooledShardConnection::Flush()
{
    TCPConnection::TryFlush();
}

bool PooledShardConnection::IsWritePending()
{
    return tcpwrite.active;
}

bool PooledShardConnection::IsConnected()
{
    return (state == CONNECTED);
}

bool PooledShardConnection::SendRequest(SDBPRequestMessage& msg)
{
    ASSERT(state == CONNECTED);

    Write(msg);

    // buffer is saturated
    if (GetWriteBuffer().GetLength() >= MESSAGING_BUFFER_THRESHOLD)
        return false;
    
    return true;
}

bool PooledShardConnection::OnMessage(ReadBuffer& msg)
{
    if (conn == NULL)
    {
        Log_Debug("message: %R", &msg);
        return false;
    }
    return conn->OnMessage(msg);
}

void PooledShardConnection::OnWrite()
{
    MessageConnection::OnWrite();

    if (conn != NULL)
        conn->OnWrite();
}

void PooledShardConnection::OnConnect()
{
    MessageConnection::OnConnect();

    if (conn != NULL)
        conn->OnConnect();
}

void PooledShardConnection::OnClose()
{
    // close the socket and try reconnecting
    MessageConnection::OnClose();

    // handle request reassign logic
    if (conn != NULL)
        conn->OnClose();

    // reconnect
    if (EventLoop::Now() - connectTime > connectTimeout.GetDelay())
    {
        // more than connectTimeout has elapsed since last connect, reconnect immediately
        Connect();
    }
    else
    {
        // wait for timeout
        EventLoop::Reset(&connectTimeout);
    }
}

PooledShardConnection::PooledShardConnection(Endpoint& endpoint_)
{
    autoFlush = false;
    endpoint = endpoint_;
    prev = next = this;
    conn = NULL;
    lastUsed = 0;
    name.Write(endpoint.ToString());
    
    ASSERT(name.GetLength() > 0);

    Connect();
}

PooledShardConnection::~PooledShardConnection()
{
    Log_Debug("Connection deleted, endpoint: %B", &name);
}
