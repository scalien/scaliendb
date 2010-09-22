#include "ControllerApp.h"
#include "System/Config.h"

void ControllerApp::Init()
{
	controller.Init();
	
	httpContext.SetController(&controller);
	httpServer.Init(configFile.GetIntValue("http.port", 8080));
	httpServer.RegisterHandler(&httpContext);
	
	sdbpServer.Init(configFile.GetIntValue("sdbp.port", 7080));
	sdbpServer.SetContext(&controller);
}
