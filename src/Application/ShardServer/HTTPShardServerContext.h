#ifndef HTTPSHARDSERVERCONTEXT_H
#define HTTPSHARDSERVERCONTEXT_H

#include "Application/HTTP/HTTPHandler.h"

class ShardServer;

/*
===============================================================================================

 HTTPControllerContext

===============================================================================================
*/

class HTTPShardServerContext : public HTTPHandler
{
public:
    void            SetShardServer(ShardServer* shardServer);
    
    // HTTPHandler interface
    virtual bool    HandleRequest(HTTPConnection* conn, HTTPRequest& request);

private:
    ShardServer*    shardServer;
};

#endif
