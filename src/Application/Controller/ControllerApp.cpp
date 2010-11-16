#include "ControllerApp.h"
#include "System/Config.h"

void ControllerApp::Init()
{
    configServer.Init();
    
    httpHandler.SetConfigServer(&configServer);
    httpServer.Init(configFile.GetIntValue("http.port", 8080));
    httpServer.RegisterHandler(&httpHandler);
    
    sdbpServer.Init(configFile.GetIntValue("sdbp.port", 7080));
    sdbpServer.SetContext(&configServer);
}

void ControllerApp::Shutdown()
{
    configServer.Shutdown();
}