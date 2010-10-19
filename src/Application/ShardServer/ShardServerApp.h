#ifndef SHARDSERVERAPP_H
#define SHARDSERVERAPP_H

#include "ShardServer.h"
#include "HTTPShardServerContext.h"
#include "Application/Common/Application.h"
#include "Application/HTTP/HTTPServer.h"
#include "Application/SDBP/SDBPServer.h"

/*
===============================================================================================

 ShardServerApp

===============================================================================================
*/

class ShardServerApp : public Application
{
public:
    // ========================================================================================
    // Application interface:
    //
    void                    Init();
    void                    Shutdown();

private:
    ShardServer             shardServer;

    HTTPServer              httpServer;
    HTTPShardServerContext  httpContext;

    SDBPServer              sdbpServer;
};

#endif
