#ifndef HTTPCONTROLLERCONTEXT_H
#define HTTPCONTROLLERCONTEXT_H

#include "Application/HTTP/HTTPServer.h"

class ConfigServer;   // forward

/*
===============================================================================================

 HTTPControllerContext

===============================================================================================
*/

class HTTPControllerContext : public HTTPHandler
{
public:
    void            SetConfigServer(ConfigServer* configServer);
    
    // HTTPHandler interface
    virtual bool    HandleRequest(HTTPConnection* conn, HTTPRequest& request);

private:
    ConfigServer*   configServer;
};

#endif
