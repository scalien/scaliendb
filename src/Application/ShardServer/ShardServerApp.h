#ifndef SHARDSERVERAPP_H
#define SHARDSERVERAPP_H

#include "ShardServer.h"
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

private:
    ShardServer             shardServer;

//    HTTPServer              httpServer;
//    HTTPShardServerContext  httpContext;

    SDBPServer              sdbpServer;
};

#endif
