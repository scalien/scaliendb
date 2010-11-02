#include "SDBPServer.h"

#define CONN_BACKLOG    10

void SDBPServer::Init(int port)
{
    if (!TCPServer<SDBPServer, SDBPConnection>::Init(port, true, CONN_BACKLOG))
        STOP_FAIL(1, "Cannot initialize SDBPServer");
}

void SDBPServer::Shutdown()
{
    Close();
}

void SDBPServer::InitConn(SDBPConnection* conn)
{
    conn->Init(this);
    conn->SetContext(context);
}

void SDBPServer::SetContext(SDBPContext* context_)
{
    context = context_;
}
