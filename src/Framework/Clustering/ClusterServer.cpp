#include "ClusterServer.h"
#include "ClusterTransport.h"

bool ClusterServer::Init(int port)
{
    return TCPServer<ClusterServer, ClusterConnection>::Init(port);
}

void ClusterServer::InitConn(ClusterConnection* conn)
{
    conn->SetTransport(transport);
    conn->InitConnected();
}

void ClusterServer::SetTransport(ClusterTransport* transport_)
{
    transport = transport_;
}

bool ClusterServer::IsManaged()
{
    return false;
}
