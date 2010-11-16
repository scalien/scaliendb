#ifndef CONTROLLERAPP_H
#define CONTROLLERAPP_H

#include "Controller.h"
#include "HTTPControllerContext.h"
#include "Application/Common/Application.h"
#include "Application/HTTP/HTTPServer.h"
#include "Application/SDBP/SDBPServer.h"

/*
===============================================================================================

 ControllerApp

===============================================================================================
*/

class ControllerApp : public Application
{
public:
    // ========================================================================================
    // Application interface:
    //
    void                    Init();
    void                    Shutdown();

private:
    ConfigServer            configServer;

    HTTPServer              httpServer;
    HTTPControllerContext   httpContext;

    SDBPServer              sdbpServer;
};

#endif
