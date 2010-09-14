#include "ControllerApp.h"
#include "System/Config.h"

void ControllerApp::Init()
{
	controller.Init();
	httpContext.SetController(&controller);
	httpServer.Init(configFile.GetIntValue("http.port", 8080));
	httpServer.RegisterHandler(&httpContext);
}
