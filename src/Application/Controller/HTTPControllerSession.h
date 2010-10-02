#ifndef HTTPCONTROLLERSESSION_H
#define HTTPCONTROLLERSESSION_H

#include "Application/Common/ClientSession.h"
#include "Application/HTTP/HTTPSession.h"
#include "HTTPControllerContext.h"

class Controller;       // forward
class ClientRequest;    // forward
class UrlParam;         // forward
class ConfigState;      // forward

/*
===============================================================================================

 HTTPControllerSession

===============================================================================================
*/

class HTTPControllerSession : public ClientSession
{
public:

    void                SetController(Controller* controller);
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
    void                PrintShardServers(ConfigState* configState);
    void                PrintQuorumMatrix(ConfigState* configState);
    void                PrintDatabases(ConfigState* configState);
    void                PrintShardMatrix(ConfigState* configState);
    void                PrintConfigState();
    bool                ProcessCommand(ReadBuffer& cmd, UrlParam& params);
    ClientRequest*      ProcessControllerCommand(ReadBuffer& cmd, UrlParam& params);
    ClientRequest*      ProcessGetMaster(UrlParam& params);
    ClientRequest*      ProcessGetState(UrlParam& params);
    ClientRequest*      ProcessCreateQuorum(UrlParam& params);
//  ClientRequest*      ProcessIncreaseQuorum(UrlParam& params);
//  ClientRequest*      ProcessDecreaseQuorum(UrlParam& params);
    ClientRequest*      ProcessCreateDatabase(UrlParam& params);
    ClientRequest*      ProcessRenameDatabase(UrlParam& params);
    ClientRequest*      ProcessDeleteDatabase(UrlParam& params);
    ClientRequest*      ProcessCreateTable(UrlParam& params);
    ClientRequest*      ProcessRenameTable(UrlParam& params);
    ClientRequest*      ProcessDeleteTable(UrlParam& params);
    
    void                OnConnectionClose();

    Controller*         controller;
    HTTPSession         session;
};

#endif
