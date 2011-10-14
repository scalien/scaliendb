#include "SDBPServer.h"

#define CONN_BACKLOG    1000

void SDBPServer::Init(int port)
{
    if (!TCPServer<SDBPServer, SDBPConnection>::Init(port, true, CONN_BACKLOG))
        STOP_FAIL(1, "Cannot initialize SDBPServer");
    useKeepAlive = false;
}

void SDBPServer::Shutdown()
{
    Close();
}

void SDBPServer::InitConn(SDBPConnection* conn)
{
    conn->UseKeepAlive(useKeepAlive);
    conn->Init(this);
    conn->SetContext(context);
}

void SDBPServer::SetContext(SDBPContext* context_)
{
    context = context_;
}

void SDBPServer::UseKeepAlive(bool useKeepAlive_)
{
    useKeepAlive = useKeepAlive_;
}
