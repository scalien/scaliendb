#include "ShardServerApp.h"
#include "System/Config.h"
#include "Application/Common/ContextTransport.h"

ShardServerApp::ShardServerApp(bool restoreMode_, bool setNodeID_, uint64_t nodeID_)
{
    restoreMode = restoreMode_;
    setNodeID = setNodeID_;
    nodeID = nodeID_;
}

void ShardServerApp::Init()
{
    int     httpPort;
    int     sdbpPort;

    httpPort = configFile.GetIntValue("http.port", 8080);
    httpContext.SetShardServer(&shardServer);
    httpServer.Init(httpPort);
    httpServer.RegisterHandler(&httpContext);

    sdbpPort = configFile.GetIntValue("sdbp.port", 7080);
    sdbpServer.Init(sdbpPort);
    sdbpServer.SetContext(&shardServer);
    sdbpServer.UseKeepAlive(true);

    // start shardServer only after network servers are started
    shardServer.Init(this, restoreMode, setNodeID, nodeID);

    Log_Message("Web admin interface started on port %d", httpPort);
    Log_Message("Waiting for connections on port %d", sdbpPort);
}

void ShardServerApp::Shutdown()
{
    sdbpServer.Shutdown();
    httpServer.Shutdown();
    shardServer.Shutdown();
}

uint64_t ShardServerApp::GetMemoryUsage()
{
    return sdbpServer.GetMemoryUsage() + httpServer.GetMemoryUsage();
}

unsigned ShardServerApp::GetNumSDBPClients()
{
    return sdbpServer.GetNumActiveConns();
}
