#ifndef HTTPCONTROLLERSESSION_H
#define HTTPCONTROLLERSESSION_H

#include "Application/Common/ClientSession.h"
#include "Application/HTTP/HTTPSession.h"
#include "Application/HTTP/UrlParam.h"
#include "HTTPControllerContext.h"

class Controller;       // forward
class ClientRequest;    // forward
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
    bool                ProcessCommand(ReadBuffer& cmd);
    ClientRequest*      ProcessControllerCommand(ReadBuffer& cmd);
    ClientRequest*      ProcessGetMaster();
    ClientRequest*      ProcessGetState();
    ClientRequest*      ProcessCreateQuorum();
//  ClientRequest*      ProcessIncreaseQuorum();
//  ClientRequest*      ProcessDecreaseQuorum();
    ClientRequest*      ProcessCreateDatabase();
    ClientRequest*      ProcessRenameDatabase();
    ClientRequest*      ProcessDeleteDatabase();
    ClientRequest*      ProcessCreateTable();
    ClientRequest*      ProcessRenameTable();
    ClientRequest*      ProcessDeleteTable();
    
    void                OnConnectionClose();

    Controller*         controller;
    HTTPSession         session;
    UrlParam            params;
};

#endif
