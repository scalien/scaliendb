#include "ShardServerApp.h"
#include "System/Config.h"

void ShardServerApp::Init()
{
    shardServer.Init();
    
    httpContext.SetShardServer(&shardServer);
    httpServer.Init(configFile.GetIntValue("http.port", 8080));
    httpServer.RegisterHandler(&httpContext);
    
    sdbpServer.Init(configFile.GetIntValue("sdbp.port", 7080));
    sdbpServer.SetContext(&shardServer);
}

void ShardServerApp::Shutdown()
{
    shardServer.Shutdown();
}