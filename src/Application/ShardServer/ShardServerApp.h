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
    ShardServerApp(bool restoreMode, bool setNodeID, uint64_t nodeID);

    // ========================================================================================
    // Application interface:
    //
    void                    Init();
    void                    Shutdown();

    uint64_t                GetMemoryUsage();
    unsigned                GetNumSDBPClients();

private:
    ShardServer             shardServer;

    HTTPServer              httpServer;
    ShardHTTPHandler        httpContext;

    SDBPServer              sdbpServer;
    bool                    restoreMode;
    bool                    setNodeID;
    uint64_t                nodeID;
};

#endif
