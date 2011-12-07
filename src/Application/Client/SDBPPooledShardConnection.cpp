#include "SDBPPooledShardConnection.h"
#include "SDBPShardConnection.h"
#include "System/Containers/HashMap.h"
#include "System/Threading/Mutex.h"

using namespace SDBPClient;

class PooledShardConnectionList
{
public:
    Buffer                                  name;
    InList<PooledShardConnection>           list;
    InTreeNode<PooledShardConnectionList>   treeNode;
};

typedef InTreeMap<PooledShardConnectionList>  ConnectionListMap;

static ConnectionListMap    connections;
static Mutex                globalMutex;
static unsigned             poolSize = 0;
static unsigned             maxPoolSize = 0;

static inline const Buffer& Key(const PooledShardConnectionList* conn)
{
    return conn->name;
}

static inline int KeyCmp(const Buffer& a, const Buffer& b)
{
    return Buffer::Cmp(a, b);
}

PooledShardConnection* PooledShardConnection::GetConnection(ShardConnection* shardConn)
{
    PooledShardConnection*      conn;
    PooledShardConnectionList*  connList;
    Buffer                      name;
    
    MutexGuard                  guard(globalMutex);

    name.Write(shardConn->GetEndpoint().ToReadBuffer());
    connList = connections.Get(name);
    if (connList == NULL)
    {
        connList = new PooledShardConnectionList();
        connList->name.Write(name);
        connections.Insert<const Buffer&>(connList);
    }
    
    if (connList->list.GetLength() == 0)
    {
        conn = new PooledShardConnection(shardConn->GetEndpoint());
        Log_Debug("New connection, nodeID: %U, endpoint: %s", shardConn->GetNodeID(), shardConn->GetEndpoint().ToString());
    }
    else
    {
        conn = connList->list.Pop();
        poolSize -= 1;
        Log_Debug("Pooled connection, nodeID: %U, endpoint: %s", shardConn->GetNodeID(), shardConn->GetEndpoint().ToString());
    }

    conn->conn = shardConn;

    // TODO: start sending requests
        
    return conn;
}

void PooledShardConnection::ReleaseConnection(PooledShardConnection* conn)
{
    PooledShardConnectionList*  connList;

    MutexGuard      guard(globalMutex);

    Log_Debug("Connection released, nodeID: %U, endpoint: %s", conn->conn->GetNodeID(), conn->conn->GetEndpoint().ToString());

    conn->conn = NULL;
    if (poolSize < maxPoolSize)
    {
        connList = connections.Get(conn->GetName());
        ASSERT(connList != NULL);
        connList->list.Prepend(conn);
        poolSize += 1;
    }
    else
        delete conn;
}

void PooledShardConnection::SetPoolSize(unsigned poolSize_)
{
    MutexGuard      guard(globalMutex);

    maxPoolSize = poolSize_;
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
        // lot of time has elapsed since last connect, reconnect immediately
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
    name.Write(endpoint.ToString());
    
    ASSERT(name.GetLength() > 0);

    Connect();
}
