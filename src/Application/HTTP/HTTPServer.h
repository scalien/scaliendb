#ifndef HTTPSERVER_H
#define HTTPSERVER_H

#include "Framework/TCP/TCPServer.h"
#include "HTTPConnection.h"
#include "HTTPConsts.h"
#include "HTTPHandler.h"

class KeyspaceDB;   // forward
class HTTPRequest;  // forward
class HTTPServer;   // forward

/*
===============================================================================================

 HTTPServer

===============================================================================================
*/

class HTTPServer : public TCPServer<HTTPServer, HTTPConnection>
{
public:
    void            Init(int port);
    void            Shutdown();
    
    void            InitConn(HTTPConnection* conn);

    void            RegisterHandler(HTTPHandler* handler);
    bool            HandleRequest(HTTPConnection* conn, HTTPRequest& request);
    
private:
    HTTPHandler*    handlers;
};

#endif
