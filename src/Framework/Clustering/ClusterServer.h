#ifndef CLUSTERSERVER_H
#define CLUSTERSERVER_H

#include "Framework/TCP/TCPServer.h"
#include "ClusterConnection.h"

/*
===============================================================================================

 ClusterServer

===============================================================================================
*/

class ClusterServer : public TCPServer<ClusterServer, ClusterConnection>
{
public:
    bool                Init(int port);
    void                InitConn(ClusterConnection* conn);
    
    void                SetTransport(ClusterTransport* transport);
    bool                IsManaged();

private:    
    ClusterTransport*   transport;
};

#endif
