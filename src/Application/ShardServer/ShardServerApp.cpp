#include "ShardServerApp.h"
#include "System/Config.h"
#include "Application/Common/ContextTransport.h"

void ShardServerApp::Init()
{
    shardServer.Init();
    
    httpContext.SetShardServer(&shardServer);
    httpServer.Init(configFile.GetIntValue("http.port", 8080));
    httpServer.RegisterHandler(&httpContext);
  
    // TODO: the correct would be if shard servers sent their sdbp.port to controller
    // instead we make a HACK
//    sdbpServer.Init(configFile.GetIntValue("sdbp.port", 7080));
    sdbpServer.Init(CONTEXT_TRANSPORT->GetSelfEndpoint().GetPort() + 1);
    sdbpServer.SetContext(&shardServer);
}

void ShardServerApp::Shutdown()
{
    shardServer.Shutdown();
}