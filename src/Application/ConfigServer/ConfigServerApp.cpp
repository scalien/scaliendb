#include "ConfigServerApp.h"
#include "System/Config.h"

void ConfigServerApp::Init()
{
    configServer.Init();
    
    httpHandler.SetConfigServer(&configServer);
    httpServer.Init(configFile.GetIntValue("http.port", 8080));
    httpServer.RegisterHandler(&httpHandler);
    
    sdbpServer.Init(configFile.GetIntValue("sdbp.port", 7080));
    sdbpServer.SetContext(&configServer);
}

void ConfigServerApp::Shutdown()
{
    sdbpServer.Shutdown();
    httpServer.Shutdown();
    configServer.Shutdown();
}