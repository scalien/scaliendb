#include "ConfigServerApp.h"
#include "System/Config.h"

void ConfigServerApp::Init()
{
    ReadBuffer  docroot;
    ReadBuffer  prefix;
    ReadBuffer  index;
    
    configServer.Init();
    
    httpHandler.SetConfigServer(&configServer);
    httpServer.Init(configFile.GetIntValue("http.port", 8080));
    httpServer.RegisterHandler(&httpHandler);
         
    docroot.Wrap(configFile.GetValue("http.documentRoot", "."));
    prefix.Wrap(configFile.GetValue("http.staticPrefix", "/webadmin"));
    index.Wrap(configFile.GetValue("http.directoryIndex", "index.html"));

    httpFileHandler.Init(docroot, prefix);
    httpFileHandler.SetDirectoryIndex(index);
    httpServer.RegisterHandler(&httpFileHandler);
    
    sdbpServer.Init(configFile.GetIntValue("sdbp.port", 7080));
    sdbpServer.SetContext(&configServer);
}

void ConfigServerApp::Shutdown()
{
    sdbpServer.Shutdown();
    httpServer.Shutdown();
    configServer.Shutdown();
}