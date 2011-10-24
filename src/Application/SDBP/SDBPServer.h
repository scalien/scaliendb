#ifndef SDBPSERVER_H
#define SDBPSERVER_H

#include "Framework/TCP/TCPServer.h"
#include "SDBPConnection.h"

class SDBPContext; // forward

/*
===============================================================================================

 SDBPServer

===============================================================================================
*/

class SDBPServer : public TCPServer<SDBPServer, SDBPConnection>
{
public:
    void            Init(int port);
    void            Shutdown();
    
    void            InitConn(SDBPConnection* conn);
    void            SetContext(SDBPContext* context);
    void            UseKeepAlive(bool useKeepAlive_);

private:
    bool            useKeepAlive;
    SDBPContext*    context;
};

#endif
