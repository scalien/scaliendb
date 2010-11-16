#ifndef CONTROLLERAPP_H
#define CONTROLLERAPP_H

#include "ConfigServer.h"
#include "ConfigHTTPHandler.h"
#include "Application/Common/Application.h"
#include "Application/HTTP/HTTPServer.h"
#include "Application/SDBP/SDBPServer.h"

/*
===============================================================================================

 ConfigServerApp

===============================================================================================
*/

class ConfigServerApp : public Application
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
    ConfigHTTPHandler       httpHandler;

    SDBPServer              sdbpServer;
};

#endif
