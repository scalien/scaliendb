#ifndef CONFIGSERVERAPP_H
#define CONFIGSERVERAPP_H

#include "ConfigServer.h"
#include "ConfigHTTPHandler.h"
#include "Application/Common/Application.h"
#include "Application/HTTP/HTTPServer.h"
#include "Application/HTTP/HTTPFileHandler.h"
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
    ConfigServerApp(bool restoreMode);

    void                    Init();
    void                    Shutdown();

    void                    SetLogStatTimeout(uint64_t timeout);
    void                    OnLogStatTimer();
    unsigned                GetNumSDBPClients();

    bool                    restoreMode;

private:
    ConfigServer            configServer;

    HTTPServer              httpServer;
    ConfigHTTPHandler       httpHandler;
    HTTPFileHandler         httpFileHandler;

    SDBPServer              sdbpServer;

    Countdown               logStatTimer;
};

#endif
