#ifndef SHARDSERVERAPP_H
#define SHARDSERVERAPP_H

#include "ShardServer.h"
#include "HTTPShardServerContext.h"
#include "Application/HTTP/HTTPServer.h"
#include "Application/SDBP/SDBPServer.h"

/*
===============================================================================================

 ShardServerApp

===============================================================================================
*/

class ShardServerApp
{
public:
    void                    Init();
    void                    Shutdown();

private:
    ShardServer             shardServer;

    HTTPServer              httpServer;
    HTTPShardServerContext  httpContext;

    SDBPServer              sdbpServer;
};

#endif
