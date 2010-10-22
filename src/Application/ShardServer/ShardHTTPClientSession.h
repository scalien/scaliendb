#ifndef HTTPSHARDSERVERSESSION_H
#define HTTPSHARDSERVERSESSION_H

#include "Application/Common/ClientSession.h"
#include "Application/HTTP/HTTPSession.h"

class ShardServer;       // forward
class ClientRequest;    // forward
class UrlParam;         // forward

/*
===============================================================================================

 ShardHTTPClientSession

===============================================================================================
*/

class ShardHTTPClientSession : public ClientSession
{
public:
    void                SetShardServer(ShardServer* shardServer);
    void                SetConnection(HTTPConnection* conn);

    bool                HandleRequest(HTTPRequest& request);

    // ========================================================================================
    // ClientSession interface
    //
    virtual void        OnComplete(ClientRequest* request, bool last);
    virtual bool        IsActive();
    // ========================================================================================

private:
    void                PrintStatus();
    bool                ProcessCommand(ReadBuffer& cmd, UrlParam& params);
    ClientRequest*      ProcessShardServerCommand(ReadBuffer& cmd, UrlParam& params);
    ClientRequest*      ProcessGetPrimary(UrlParam& params);
    ClientRequest*      ProcessGet(UrlParam& params);
    ClientRequest*      ProcessSet(UrlParam& params);
    ClientRequest*      ProcessDelete(UrlParam& params);
    void                OnConnectionClose();

    ShardServer*        shardServer;
    HTTPSession         session;

};

#endif
