#ifndef CONFIGHTTPHANDLER_H
#define CONFIGHTTPHANDLER_H

#include "Application/HTTP/HTTPServer.h"

class ConfigServer;   // forward

/*
===============================================================================================

 ConfigHTTPHandler

===============================================================================================
*/

class ConfigHTTPHandler : public HTTPHandler
{
public:
    void            SetConfigServer(ConfigServer* configServer);
    
    // HTTPHandler interface
    virtual bool    HandleRequest(HTTPConnection* conn, HTTPRequest& request);

private:
    ConfigServer*   configServer;
};

#endif
