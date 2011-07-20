#ifndef SHARDSERVERAPP_H
#define SHARDSERVERAPP_H

#include "ShardServer.h"
#include "ShardHTTPHandler.h"
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

    uint64_t                GetMemoryUsage();

private:
    ShardServer             shardServer;

    HTTPServer              httpServer;
    ShardHTTPHandler        httpContext;

    SDBPServer              sdbpServer;
};

#endif
