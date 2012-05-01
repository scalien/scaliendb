#ifndef SHARDHTTPCLIENTSESSION_H
#define SHARDHTTPCLIENTSESSION_H

#include "System/Events/Countdown.h"
#include "Application/Common/ClientSession.h"
#include "Application/HTTP/HTTPSession.h"
#include "Application/HTTP/UrlParam.h"

class ShardServer;      // forward
class ClientRequest;    // forward

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
    void                PrintStorage();
    void                PrintStatistics();
    void                PrintMemoryState();
    void                PrintConfigFile();

    bool                ProcessCommand(ReadBuffer& cmd);
    void                ProcessDebugCommand();
    void                ProcessStartBackup();
    void                ProcessEndBackup();
    void                ProcessDumpMemoChunks();
    bool                ProcessSettings();
    void                ProcessClearCache();
    ClientRequest*      ProcessShardServerCommand(ReadBuffer& cmd);
    ClientRequest*      ProcessGetPrimary();
    ClientRequest*      ProcessGet();
    ClientRequest*      ProcessSet();
    ClientRequest*      ProcessSetIfNotExists();
    ClientRequest*      ProcessTestAndSet();
    ClientRequest*      ProcessTestAndDelete();
    ClientRequest*      ProcessGetAndSet();
    ClientRequest*      ProcessAdd();
    ClientRequest*      ProcessDelete();
    ClientRequest*      ProcessRemove();
    ClientRequest*      ProcessListKeys();
    ClientRequest*      ProcessListKeyValues();
    ClientRequest*      ProcessCount();    
    void                OnConnectionClose();
    bool                GetRedirectedShardServer(uint64_t tableID, const ReadBuffer& key, Buffer& location);
    
    void                OnTraceOffTimeout();

    ShardServer*        shardServer;
    HTTPSession         session;
    UrlParam            params;
    bool                binaryData;
    Countdown           onTraceOffTimeout;
};

#endif
