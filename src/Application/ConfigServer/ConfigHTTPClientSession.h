#ifndef CONFIGHTTPCLIENTSESSION_H
#define CONFIGHTTPCLIENTSESSION_H

#include "Application/Common/ClientSession.h"
#include "Application/HTTP/HTTPSession.h"
#include "Application/HTTP/UrlParam.h"
#include "ConfigHTTPHandler.h"

class ConfigServer;     // forward
class ClientRequest;    // forward
class ConfigState;      // forward

/*
===============================================================================================

 ConfigHTTPClientSession

===============================================================================================
*/

class ConfigHTTPClientSession : public ClientSession
{
public:

    void                SetConfigServer(ConfigServer* configServer);
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
    void                PrintShards(ConfigState* configState);
    void                PrintQuorumMatrix(ConfigState* configState);
    void                PrintDatabases(ConfigState* configState);
    void                PrintShardMatrix(ConfigState* configState);
    void                PrintConfigState();
    void                PrintConfigFile();

    void                ProcessActivateShardServer();
    void                ProcessSettings();
    bool                ProcessCommand(ReadBuffer& cmd);
    ClientRequest*      ProcessConfigCommand(ReadBuffer& cmd);
    ClientRequest*      ProcessGetMaster();
    ClientRequest*      ProcessGetMasterHTTP();
    ClientRequest*      ProcessGetState();
    ClientRequest*      ProcessPollConfigState();
    ClientRequest*      ProcessUnregisterShardServer();
    ClientRequest*      ProcessCreateQuorum();
    ClientRequest*      ProcessRenameQuorum();
    ClientRequest*      ProcessDeleteQuorum();
    ClientRequest*      ProcessAddShardServerToQuorum();
    ClientRequest*      ProcessRemoveShardServerFromQuorum();
    ClientRequest*      ProcessCreateDatabase();
    ClientRequest*      ProcessRenameDatabase();
    ClientRequest*      ProcessDeleteDatabase();
    ClientRequest*      ProcessCreateTable();
    ClientRequest*      ProcessRenameTable();
    ClientRequest*      ProcessDeleteTable();
    ClientRequest*      ProcessTruncateTable();
    ClientRequest*      ProcessFreezeTable();
    ClientRequest*      ProcessUnfreezeTable();
    ClientRequest*      ProcessSplitShard();
    ClientRequest*      ProcessMigrateShard();
    
    void                OnConnectionClose();

    ConfigServer*       configServer;
    HTTPSession         session;
    UrlParam            params;
    unsigned            numResponses;   // TODO: remove after testing
};

#endif
